#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Schema inference checks the header of a NetCDF file against the size of the file. It needs only
# the size, not random access, so the check must not be skipped for an HTTP server that reports
# `Content-Length` but does not support range requests: otherwise `DESCRIBE` accepts (and caches)
# the schema of a file that `SELECT` rejects.
#
# The file below is a CDF-1 file with the dimension `x` of the length 3 and the variable `int v(x)`,
# truncated right after the header. The server sends it without `Accept-Ranges`.
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 - "$HTTP_PORT" <<'PYTHON' &
import struct
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

def tag(value):
    return struct.pack('>i', value)

def name(value):
    data = value.encode()
    return tag(len(data)) + data + b'\x00' * ((4 - len(data) % 4) % 4)

NC_DIMENSION, NC_VARIABLE, NC_INT, ABSENT = 10, 11, 4, tag(0) + tag(0)

def header(begin_of_v):
    result = b'CDF\x01' + tag(0)
    result += tag(NC_DIMENSION) + tag(1) + name('x') + tag(3)
    result += ABSENT
    result += tag(NC_VARIABLE) + tag(1)
    result += name('v') + tag(1) + tag(0) + ABSENT + tag(NC_INT) + tag(12) + tag(begin_of_v)
    return result

CONTENT = header(len(header(0)))

class Handler(BaseHTTPRequestHandler):
    def send_headers(self):
        self.send_response(200)
        self.send_header('Content-Length', str(len(CONTENT)))
        self.end_headers()

    def do_HEAD(self):
        self.send_headers()

    def do_GET(self):
        self.send_headers()
        self.wfile.write(CONTENT)

    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
PYTHON
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 100); do
    curl -s "http://127.0.0.1:$HTTP_PORT/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

URL="http://127.0.0.1:$HTTP_PORT/${CLICKHOUSE_DATABASE}.nc"

$CLICKHOUSE_LOCAL -q "DESCRIBE url('$URL', NetCDF)" 2>&1 | grep -o -m1 "does not fit in the NetCDF file"
$CLICKHOUSE_LOCAL -q "SELECT * FROM url('$URL', NetCDF)" 2>&1 | grep -o -m1 "does not fit in the NetCDF file"
