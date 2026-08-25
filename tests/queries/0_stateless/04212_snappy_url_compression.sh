#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that `compression_method='snappy'` round-trips through `url()` for both
# values of the `snappy_mode` setting, and that mismatched modes fail to decode.
# This mirrors `04201_snappy_file_compression.sh` for `StorageURL`.

DIR="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"
trap 'rm -rf "${DIR}"; kill ${HTTP_PID} 2>/dev/null; wait ${HTTP_PID} 2>/dev/null' EXIT

FRAMED_FILE="${DIR}/framed.tsv.snappy"

# Produce a framed-snappy payload using `file()` (already covered by 04201).
${CLICKHOUSE_CLIENT} -q "
INSERT INTO TABLE FUNCTION file('${FRAMED_FILE}', 'TSV', 'x UInt32', 'snappy')
SELECT number FROM numbers(5)
SETTINGS snappy_mode = 'framed';
"

# Start a tiny HTTP server that serves the generated framed-snappy payload.
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

with open('${FRAMED_FILE}', 'rb') as f:
    PAYLOAD = f.read()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Length', str(len(PAYLOAD)))
        self.end_headers()
        self.wfile.write(PAYLOAD)
    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

# Wait for the server to start.
for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/framed.tsv.snappy" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# Reading framed-snappy via `url()` with `snappy_mode='framed'` must succeed.
${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/framed.tsv.snappy', 'TSV', 'x UInt32', 'snappy')
ORDER BY x
SETTINGS snappy_mode = 'framed';
"

# Reading framed-snappy via `url()` under default `snappy_mode='basic'` must
# fail with a decode error, not silently accept the wrong wire format.
if ${CLICKHOUSE_CLIENT} -q "
SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/framed.tsv.snappy', 'TSV', 'x UInt32', 'snappy')
ORDER BY x;
" 2>&1 | grep -qE "SNAPPY_UNCOMPRESS_FAILED|Cannot read all data"
then
    echo "OK: framed payload rejected by basic url() reader"
else
    echo "FAIL: framed payload was not rejected by basic url() reader" >&2
    exit 1
fi
