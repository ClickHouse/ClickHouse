#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a query over the `url` function with `max_execution_time` and the `break` overflow mode
# returns the rows it has already read when the soft timeout interrupts an HTTP retry backoff,
# instead of waiting out the remaining attempts and failing with the error of the last one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which serves a complete file at /good and always answers /bad with a retriable error, so
# that a query over both gets its rows from the first file and then hangs in the retry loop of
# `ReadWriteBufferFromHTTP` on the second. It binds to the port 0 and reports the port the kernel gave
# it, so that it cannot collide with anything else running in parallel.
python3 -u -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/good':
            body = b'1\n2\n3\n4\n5\n'
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
        else:
            self.send_error(503)

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

QUERY="SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/{good,bad}', 'CSV', 'x UInt64')"

# 30 attempts with a backoff of 1 to 2 seconds for the failing file: minutes of retrying, while the
# soft timeout of 1 second must interrupt the backoff and return the rows of the file that succeeded.
# On a loaded machine the timeout can legitimately fire before the first file has streamed anything,
# leaving a valid but empty partial result, so retry until the rows make it through - the guarantees
# under test (the query succeeds and returns quickly) must hold on every attempt.
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

for _ in {1..10}; do
    STARTED_AT=$EPOCHSECONDS
    RESULT=$($CLICKHOUSE_CLIENT \
        --http_max_tries 30 \
        --http_retry_initial_backoff_ms 1000 \
        --http_retry_max_backoff_ms 2000 \
        --max_execution_time 1 \
        --timeout_overflow_mode break \
        --query "$QUERY" 2>"$ERROR_FILE")
    CLIENT_STATUS=$?
    ELAPSED=$((EPOCHSECONDS - STARTED_AT))

    ((CLIENT_STATUS == 0)) && ((ELAPSED < 30)) && [[ -n "$RESULT" ]] && break
done

if ((CLIENT_STATUS == 0)); then
    echo "the timed out query succeeded"
else
    echo "FAIL: the timed out query failed: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

echo "$RESULT"

if ((ELAPSED < 30)); then
    echo "returned the partial result soon after the timeout"
else
    echo "FAIL: the query kept running for $ELAPSED seconds"
fi

# A query which is not interrupted must still report the error of the server as it did before.
if $CLICKHOUSE_CLIENT \
    --http_max_tries 2 \
    --http_retry_initial_backoff_ms 10 \
    --http_retry_max_backoff_ms 20 \
    --query "$QUERY" 2>&1 | grep -q -F "HTTP status code: 503"; then
    echo "the error of the server is reported when the query is not interrupted"
else
    echo "FAIL: the error of the server is not reported"
fi
