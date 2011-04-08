package yourpackagehere;
import java.io.*;
import java.net.Socket;

public class KestrelClient {

    public class ParseError extends Exception {
        public ParseError() { super("Parse Error"); }
        public ParseError(String msg) { super(msg); }
    }

    public class Item {
        public Item(byte[] data, int id) {
            _id = id;
            _data = data;
        }
        public byte[] _data;
        public int _id;
    };

    private class MixedInputStream { // For dual binary and ASCII text streams.
        private InputStream _is = null;
        public MixedInputStream(InputStream is) {
            _is = is;
        }

        public byte[] readFixedLength(int num_bytes)
            throws IOException, ParseError {
            byte[] data = new byte[num_bytes];
            for(int i = 0; i < num_bytes; i++) {
                int val = _is.read();
                if( val == -1 ) {
                    throw new ParseError("EOF while expecting fixed length bytes");
                }
                else { data[i] = (byte)val; }
            }
            return data;
        }

        public String readLine() 
            throws ParseError, IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();

            int newline_byte_count = 0;
            while(true) {
                int val = _is.read();
                if(val == -1) {
                    throw new ParseError("EOF while expecting line.");
                } else if(val == 13) { // check for \r?
                    newline_byte_count++;
                    _is.read();  // expecting \n
                    break;
                } else {
                    baos.write( val );
                }
            }
            return new String( baos.toByteArray() );
        }
    }

    private BufferedReader _rdr = null;
    private InputStream _is = null;
    private OutputStream _os = null;
    private MixedInputStream _mis = null;
    private Socket _sock = null;
    private String _queueName = "";

    public KestrelClient(String hostname, int port, String queueName) 
        throws IOException {
        _sock = new Socket(hostname, port);
        _is = _sock.getInputStream();
        _mis = new MixedInputStream( _is );
        _os = _sock.getOutputStream();
        _rdr = new BufferedReader(new InputStreamReader(_is), 1);
        _queueName = queueName;
    }

    public void close() 
        throws IOException {
        _rdr.close();
        _is.close();
        _sock.close();
        _is = null;
        _mis = null;
        _sock = null;
        _rdr = null;
    }

    private byte[] dequeueCommand() { 
        return ("GET " + _queueName + "/syn\r\n").getBytes();
    }

    private byte[] failCommand(int id) { 
        return ("GET " + _queueName + "/fail/id=" + id + "\r\n").getBytes();
    }

    private byte[] ackCommand(int id) { 
        return ("GET " + _queueName + "/ack/id=" + id + "\r\n").getBytes();
    }

    private void protocolExpect(String expected, String actual)
        throws ParseError {
        if(!expected.equals(actual)) {
            throw new ParseError("Protocol Violation: Expected `" + expected + 
                                 "`; Actual: `" + actual + "`");
        }
    }

    private Item parseDequeueOutputItem(String[] header_tokens) 
        throws IOException, ParseError {

        if(header_tokens.length != 4) { throw new ParseError(); }
        int num_bytes = Integer.parseInt(header_tokens[3]);

        String id_line = _mis.readLine(); // check null
        String[] id_line_tokens = id_line.split("\\s");
        protocolExpect("ID", id_line_tokens[0]);
        int id = Integer.parseInt(id_line_tokens[1]);

        byte[] data = _mis.readFixedLength(num_bytes);
        protocolExpect(_mis.readLine(), "");

        String footer = _mis.readLine();
        protocolExpect("END", footer);

        return new Item(data, id);
    }

    private boolean parseBooleanOutput(String expected) 
        throws IOException, ParseError {
        String header = _mis.readLine();
        if(header.equals("END")) { 
            return false;
        } else if(header.equals(expected)) {
            return true;
        } else {  // Typically CLIENT_ERROR is returned here.
            throw new ParseError("Unknown Kestrel Server Response: " + header);
        }
    }

    private Item parseDequeueOutput()
        throws IOException, ParseError {
        String header = _mis.readLine();
        String[] header_tokens = header.split("\\s");
        if(header_tokens[0].equals("END")) {
            return null; // No object on the Q.
        } else if(header_tokens[0].equals("VALUE")) {
            return parseDequeueOutputItem(header_tokens); 
        } else {  // Typically CLIENT_ERROR is returned here.
            throw new ParseError("Kestrel Server Response: " + header);
        }
    }

    public Item dequeue() 
        throws IOException, ParseError {
        _os.write(dequeueCommand());
        return parseDequeueOutput();
    }

    public boolean fail(int id)
        throws ParseError, IOException {
        _os.write(failCommand(id));
        return parseBooleanOutput("TRANSACTION_FAIL");
    }

    public boolean ack(int id) 
        throws ParseError, IOException {
        _os.write(ackCommand(id));
        return parseBooleanOutput("TRANSACTION_ACK");
    }
}
