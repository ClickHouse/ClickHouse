#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY cancels HTTP requests during HEAD phase in url() function.
# This test specifically verifies cancellation during the metadata HEAD request,
# which happens before the data GET request.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="kill_query_url_head_${CLICKHOUSE_DATABASE}_$RANDOM"
log_file=$(mktemp "./04078.XXXXXX.log")

# Get free port
HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# Start HTTP server - HEAD is slow (30s), GET is fast
# This tests cancellation during HEAD metadata phase
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
import time

class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        # Slow HEAD request - this triggers the HEAD phase retries
        time.sleep(30)
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        elif self.path == '/sample-data':
            # Fast GET - we want to test cancellation during HEAD, not GET
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'1\n')

    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$log_file"' EXIT

# Wait for server to start
for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

# Run query - server will take 30 seconds for HEAD request
# With fix: cancellation is checked during HEAD retries
# Without fix: waits for all retries to exhaust
$CLICKHOUSE_CLIENT \
    --http_make_head_request=1 \
    --http_max_tries=10 \
    --http_retry_initial_backoff_ms=500 \
    --http_retry_max_backoff_ms=1000 \
    --query_id="$query_id" \
    --query "
        SELECT count()
        FROM url('http://127.0.0.1:$HTTP_PORT/sample-data', 'CSV', 'x UInt64')
        FORMAT Null
    " >/dev/null 2>"$log_file" &
CLIENT_PID=$!

# Wait for the query to start making HTTP attempts
wait_for_query_to_start "$query_id"

# Use async KILL (without SYNC) to avoid blocking if propagation is slow.
$CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

wait $CLIENT_PID

# Verify that the specific error message "Query was cancelled during HTTP request" is present.
# This proves that our fix works - the cancellation is detected during HEAD retries,
# not after exhausting all retries.
if grep -q "Query was cancelled during HTTP request" "$log_file"; then
    echo "OK"
else
    cat "$log_file"
    echo "FAIL: Expected 'Query was cancelled during HTTP request' error message"
    exit 1
fi

rm -f "$log_file"
