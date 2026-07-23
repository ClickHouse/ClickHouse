#!/usr/bin/env bash
# Tags: no-fasttest

# Test that requesting a non-existent .gz URL with a glob pattern
# does not produce a misleading "inflate failed: buffer error" message.
# With http_skip_not_found_url_for_globs = 1 (default), it should return no rows.
# https://github.com/ClickHouse/ClickHouse/issues/49231

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Start a simple HTTP server that always returns 404.
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b'Not Found')
    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

# Wait for the server to start.
for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# All URLs return 404. With the default http_skip_not_found_url_for_globs = 1,
# the query should return 0 rows without error, not "inflate failed".

echo "--- gzip ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.gz', CSV, 'x UInt64')
"

echo "--- zstd ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.zst', CSV, 'x UInt64')
"

echo "--- brotli ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.br', CSV, 'x UInt64')
"

echo "--- lz4 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.lz4', CSV, 'x UInt64')
"

echo "--- lzma ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.xz', CSV, 'x UInt64')
"

echo "--- bzip2 ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM url('http://127.0.0.1:$HTTP_PORT/{a,b,c}.csv.bz2', CSV, 'x UInt64')
"
