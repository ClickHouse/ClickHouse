#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan, long
# Test that KILL QUERY cancels HTTP requests in url() function early.
# Tests cancellation during both HEAD and GET phases, with different
# engine_url_skip_empty_files settings.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# test_url_cancellation <head_sleep> <get_sleep> <skip_empty>
#   head_sleep: seconds to sleep in do_HEAD() (slow = test HEAD cancellation, fast = skip to GET)
#   get_sleep: seconds to sleep in do_GET() (slow = test GET cancellation)
#   skip_empty: engine_url_skip_empty_files setting value (0 or 1)
test_url_cancellation()
{
    local head_sleep=$1
    local get_sleep=$2
    local skip_empty=$3

    local query_id="kill_query_url_${CLICKHOUSE_DATABASE}_$RANDOM"
    local log_file=$(mktemp "./04078.XXXXXX.log")

    # Get free port
    local HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

    # Start HTTP server with configurable sleep times
    python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler
import time

HEAD_SLEEP = $head_sleep
GET_SLEEP = $get_sleep

class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        if HEAD_SLEEP > 0:
            time.sleep(HEAD_SLEEP)
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
            if GET_SLEEP > 0:
                time.sleep(GET_SLEEP)
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'1\n')

    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
    local HTTP_PID=$!

    # Unconditional cleanup on any function return (success, failure, or exception)
    trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$log_file"' RETURN

    # Wait for server to start
    for _ in $(seq 1 50); do
        curl -s "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
        sleep 0.1
    done

    # Run query with configurable settings
    $CLICKHOUSE_CLIENT \
        --http_make_head_request=1 \
        --http_max_tries=10 \
        --http_retry_initial_backoff_ms=500 \
        --http_retry_max_backoff_ms=1000 \
        --engine_url_skip_empty_files=$skip_empty \
        --query_id="$query_id" \
        --query "
            SELECT count()
            FROM url('http://127.0.0.1:$HTTP_PORT/sample-data', 'CSV', 'x UInt64')
            FORMAT Null
        " >/dev/null 2>"$log_file" &
    local CLIENT_PID=$!

    # Wait for the query to start making HTTP attempts
    wait_for_query_to_start "$query_id"

    # Use async KILL (without SYNC) to avoid blocking if propagation is slow.
    $CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "KILL QUERY WHERE query_id = '$query_id'" >/dev/null

    wait $CLIENT_PID

    # Verify that a cancellation error message is present.
    # Accept both "Query was cancelled during HTTP request" (cancellation during HTTP phase)
    # and "killed in pending state" (cancellation before HTTP phase started).
    if grep -q "Query was cancelled during HTTP request\|killed in pending state" "$log_file"; then
        echo "OK"
    else
        cat "$log_file"
        echo "FAIL: Expected cancellation error message"
        return 1
    fi

}

# Test a: slow HEAD, skip_empty=0, fast GET - tests HEAD cancellation phase
test_url_cancellation 30 0 0 || exit 1

# Test b: slow HEAD, skip_empty=1, fast GET - tests HEAD cancellation with skip empty files
test_url_cancellation 30 0 1 || exit 1

# Test c: fast HEAD, skip_empty=0, slow GET - tests GET cancellation phase
test_url_cancellation 0 30 0 || exit 1