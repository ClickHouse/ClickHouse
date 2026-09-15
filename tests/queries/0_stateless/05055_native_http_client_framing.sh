#!/usr/bin/env bash
# Tags: no-fasttest

# The HTTP client reads and writes message bodies through `ReadBuffer`/`WriteBuffer` directly on
# the socket. This checks the framing of both directions against a server that speaks every
# variant: a response with `Content-Length`, a response with chunked transfer encoding split into
# chunks of every size, and a request body that the client sends chunked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import http.server, socketserver

ROWS = 100000
BODY = ''.join('%d\n' % i for i in range(ROWS)).encode()

# What the last upload looked like, reported back through /result.
last_upload = 'none\t0\n'

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def send_body(self, data):
        self.send_response(200)
        self.send_header('Content-Type', 'text/tab-separated-values')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == '/result':
            self.send_body(last_upload.encode())
        elif self.path == '/chunked':
            # Chunks of growing size, so that a chunk boundary falls inside a read of the client
            # and a read of the client ends inside a chunk.
            self.send_response(200)
            self.send_header('Content-Type', 'text/tab-separated-values')
            self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            position = 0
            size = 1
            while position < len(BODY):
                piece = BODY[position:position + size]
                position += len(piece)
                size = min(size * 3, 100000)
                self.wfile.write(b'%x\r\n' % len(piece) + piece + b'\r\n')
            self.wfile.write(b'0\r\n\r\n')
        else:
            self.send_body(BODY)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/tab-separated-values')
        self.send_header('Content-Length', str(len(BODY)))
        self.end_headers()

    def do_POST(self):
        global last_upload

        received = 0
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            encoding = 'chunked'
            while True:
                size = int(self.rfile.readline().split(b';')[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                received += len(self.rfile.read(size))
                self.rfile.readline()
        else:
            encoding = 'content-length'
            rest = int(self.headers.get('Content-Length', 0))
            received = rest
            while rest > 0:
                rest -= len(self.rfile.read(min(rest, 1 << 20)))

        last_upload = '%s\t%d\n' % (encoding, received)
        self.send_body(b'')

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

Server(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null' EXIT

for _ in $(seq 1 100); do
    curl -s "http://127.0.0.1:$HTTP_PORT/result" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

URL="http://127.0.0.1:$HTTP_PORT"

echo "--- response with Content-Length ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM url('$URL/plain', TSV, 'x UInt64')"

echo "--- response with chunked transfer encoding ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM url('$URL/chunked', TSV, 'x UInt64')"

echo "--- a read buffer smaller than the chunks changes nothing ---"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM url('$URL/chunked', TSV, 'x UInt64') SETTINGS max_read_buffer_size = 137"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM url('$URL/plain', TSV, 'x UInt64') SETTINGS max_read_buffer_size = 137"

echo "--- request body that fits into one chunk ---"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION url('$URL/upload', TSV, 'x UInt64') SELECT number FROM numbers(3)"
${CLICKHOUSE_CLIENT} --query "
    SELECT encoding, size = (SELECT sum(length(toString(number)) + 1) FROM numbers(3))
    FROM url('$URL/result', TSV, 'encoding String, size UInt64')"

echo "--- request body that spans several chunks ---"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION url('$URL/upload', TSV, 'x UInt64') SELECT number FROM numbers(1000000)"
${CLICKHOUSE_CLIENT} --query "
    SELECT encoding, size = (SELECT sum(length(toString(number)) + 1) FROM numbers(1000000))
    FROM url('$URL/result', TSV, 'encoding String, size UInt64')"
