#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# AvroConfluent reads an Avro map into `Map(DateTime, String)` the same way Avro does.

SCHEMA='{"type":"record","name":"r","fields":[{"name":"id","type":"long"},{"name":"m","type":{"type":"map","values":"string"}}]}'

PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# A schema registry that serves the schema above for any id.
python3 -c "
import json, sys
from http.server import HTTPServer, BaseHTTPRequestHandler
body = json.dumps({'schema': sys.argv[1]}).encode()
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
HTTPServer(('127.0.0.1', $PORT), Handler).serve_forever()
" "$SCHEMA" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$PORT/schemas/ids/1" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# Three messages framed with magic byte 0 and schema id 1.
python3 -c "
import struct, sys
def zz(v):
    v = (v << 1) ^ (v >> 63)
    out = bytearray()
    while True:
        b = v & 0x7f
        v >>= 7
        out.append(b | 0x80 if v else b)
        if not v:
            return bytes(out)
def s(x):
    return zz(len(x)) + x.encode()
out = bytearray()
for rid, m in [(1, {'2020-01-02 03:04:05': 'a', '2021-06-07 08:09:10': 'b'}), (2, {'1999-12-31 23:59:59': 'c'}), (3, {})]:
    out += b'\x00' + struct.pack('>I', 1) + zz(rid)
    if m:
        out += zz(len(m)) + b''.join(s(k) + s(v) for k, v in m.items())
    out += zz(0)
sys.stdout.buffer.write(bytes(out))
" | $CLICKHOUSE_LOCAL --input-format AvroConfluent --format_avro_schema_registry_url "http://127.0.0.1:$PORT" \
    -S "id Int64, m Map(DateTime('UTC'), String)" -q "SELECT id, m FROM table ORDER BY id"
