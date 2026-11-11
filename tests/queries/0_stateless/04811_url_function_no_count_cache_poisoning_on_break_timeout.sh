#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a read over the `url` function interrupted by a soft `max_execution_time` with the
# `break` overflow mode does not record the rows it happened to read as the row count of the file
# in the count cache (`use_cache_for_count_from_files`), which would make later `count()` queries
# over the same URL under-report. A complete read must still populate the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which slowly streams CSV rows at /stream, flushing each row with a small delay, so that
# the stream outlives the soft timeout of the first query by a wide margin and the query is
# guaranteed to be cancelled before the end of the file, and serves a complete small file at
# /file. It binds to the port 0 and reports the port the kernel gave it, so that it cannot collide
# with anything else running in parallel.
python3 -u -c "
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/stream':
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.end_headers()
            if head:
                return
            try:
                for i in range(800):
                    self.wfile.write(b'1\n')
                    self.wfile.flush()
                    time.sleep(0.005)
            except (BrokenPipeError, ConnectionResetError):
                pass
        elif self.path == '/file':
            body = b'1\n' * 5
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
        else:
            self.send_error(404)

    def do_HEAD(self):
        self.respond(head=True)

    def do_GET(self):
        self.respond(head=False)

    def log_message(self, *args):
        pass

server = HTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

URL="http://127.0.0.1:$HTTP_PORT/stream"

# The soft timeout cancels the query strictly in the middle of the file (the small max_block_size
# keeps the chunks fine-grained if the body arrives incrementally). The rows the query returns
# depend on timing, so they are discarded; what matters is that the interrupted read succeeds and
# leaves no entry in the count cache.
if $CLICKHOUSE_CLIENT \
    --max_block_size 10 \
    --max_execution_time 1 \
    --timeout_overflow_mode break \
    --use_cache_for_count_from_files 1 \
    --query "SELECT x FROM url('$URL', 'CSV', 'x UInt64')" > /dev/null; then
    echo "the interrupted query succeeded"
else
    echo "FAIL: the interrupted query failed"
fi

$CLICKHOUSE_CLIENT --query "SELECT 'cache entries after the interrupted read:', count() FROM system.schema_inference_cache WHERE source = '$URL'"

# A complete read must still cache the row count of the file.
FILE_URL="http://127.0.0.1:$HTTP_PORT/file"
$CLICKHOUSE_CLIENT \
    --use_cache_for_count_from_files 1 \
    --query "SELECT 'rows read by the complete read:', count() FROM url('$FILE_URL', 'CSV', 'x UInt64')"

$CLICKHOUSE_CLIENT --query "SELECT 'cached row count after the complete read:', number_of_rows FROM system.schema_inference_cache WHERE source = '$FILE_URL'"
