#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Avro format is not built in the fast test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# AvroConfluent reads an Avro map into `Map(DateTime, String)` the same way Avro does.

SCHEMA='{"type":"record","name":"r","fields":[{"name":"id","type":"long"},{"name":"m","type":{"type":"map","values":"string"}}]}'

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX.port")

# A schema registry that serves the schema above for any id. It binds to port 0 and reports the port
# it got, so parallel runs cannot collide.
python3 -c "
import json, os, sys
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
server = HTTPServer(('127.0.0.1', 0), Handler)
with open(sys.argv[2] + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[2] + '.tmp', sys.argv[2])
server.serve_forever()
" "$SCHEMA" "$PORT_FILE" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in $(seq 1 300); do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
PORT=$(cat "$PORT_FILE")

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
