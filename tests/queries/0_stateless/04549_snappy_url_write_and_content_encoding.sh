#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `url()` sink always advertises the compression with the `Content-Encoding` header, so a
# snappy body must use the standardized snappy framing format regardless of the `snappy_mode`
# setting. On the read side, a response with `Content-Encoding: snappy` must likewise be decoded
# as framed snappy even under the default `snappy_mode = 'basic'`.
#
# This complements `04212_snappy_url_compression.sh`, which only covers the GET/read path where
# the compression is inferred from the URL path (and thus follows `snappy_mode`).

# Pick a free port.
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

trap 'kill ${HTTP_PID} 2>/dev/null; wait ${HTTP_PID} 2>/dev/null' EXIT

# Tiny HTTP server:
#   * POST /data       -> stores the request body (used by INSERT ... url()).
#   * GET  /data.snappy-> serves the stored body as-is (compression inferred from the path).
#   * GET  /data_ce    -> serves the stored body with `Content-Encoding: snappy` (header-driven).
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

STORE = {'body': b''}

def read_body(handler):
    if handler.headers.get('Transfer-Encoding', '').lower() == 'chunked':
        data = b''
        while True:
            size_line = handler.rfile.readline().strip()
            size = int(size_line.split(b';')[0], 16)
            if size == 0:
                handler.rfile.readline()
                break
            data += handler.rfile.read(size)
            handler.rfile.readline()
        return data
    length = int(handler.headers.get('Content-Length', 0))
    return handler.rfile.read(length)

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        STORE['body'] = read_body(self)
        self.send_response(200)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        body = STORE['body']
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        if self.path.startswith('/data_ce'):
            self.send_header('Content-Encoding', 'snappy')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

# Wait for the server to start.
for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:${HTTP_PORT}/data.snappy" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# Write 5 rows with `compression_method='snappy'` under the DEFAULT `snappy_mode = 'basic'`.
# The sink must still emit framed snappy because it sends `Content-Encoding: snappy`.
${CLICKHOUSE_CLIENT} -q "
INSERT INTO TABLE FUNCTION url('http://127.0.0.1:${HTTP_PORT}/data', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5);
"

# 1. Reading back a response with `Content-Encoding: snappy` must succeed under the DEFAULT
#    `snappy_mode = 'basic'`: the header forces the framing format.
echo "-- read with Content-Encoding: snappy under default snappy_mode:"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/data_ce', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
"

# 2. Reading back the same body with the compression inferred from the path (no header) must
#    succeed only with `snappy_mode = 'framed'` — this proves the written body is framed snappy.
echo "-- read path-inferred snappy with snappy_mode='framed':"
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/data.snappy', 'TSV', 'x UInt32', 'snappy')
ORDER BY x
SETTINGS snappy_mode = 'framed';
"

# 3. Reading the same path-inferred body under default `snappy_mode = 'basic'` must fail to decode,
#    confirming the written wire format is framed and not the Hadoop snappy block format.
if ${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/data.snappy', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
" 2>&1 | grep -qE "SNAPPY_UNCOMPRESS_FAILED|Cannot read all data"
then
    echo "OK: written body is framed snappy (rejected by basic reader)"
else
    echo "FAIL: written body was not framed snappy" >&2
    exit 1
fi
