#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a cancellation stops `StorageURLSource::initialize`, whose helpers swallow the errors of
# the requests they make: after a soft `max_execution_time` timeout with the `break` overflow mode
# interrupts a retry backoff, initialization must not go on probing failover options or downloading
# the data, because no one needs it anymore.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /data answers HEAD with a retriable error but serves GET normally: a query over it hangs in the
#   retry loop of the HEAD request for the file metadata inside `StorageURLSource::initialize`.
# - /fail1 and /fail2 always answer with a retriable error: a query over 'fail1|fail2' hangs in the
#   retry loop of the first failover option.
python3 -u -c "
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

counts = {}

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        method = 'HEAD' if head else 'GET'
        counts[f'{method} {self.path}'] = counts.get(f'{method} {self.path}', 0) + 1
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/stats':
            body = json.dumps(counts).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
        elif self.path == '/data' and not head:
            body = b'1\n2\n3\n4\n5\n'
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
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

# 30 attempts with a backoff of 1 to 2 seconds: minutes of retrying, while the soft timeout of
# 1 second must interrupt the backoff and stop the initialization.
run_interrupted_query()
{
    $CLICKHOUSE_CLIENT \
        --http_max_tries 30 \
        --http_retry_initial_backoff_ms 1000 \
        --http_retry_max_backoff_ms 2000 \
        --max_execution_time 1 \
        --timeout_overflow_mode break \
        --query "$1"
}

stat_count()
{
    curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('$1', 0))"
}

# 1. The timeout interrupts the retries of the HEAD request for the file metadata. The query must
# not go on downloading the data of /data - no rows in the result and no GET requests to the server.
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")
STARTED_AT=$EPOCHSECONDS
RESULT=$(run_interrupted_query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" 2>"$ERROR_FILE")
CLIENT_STATUS=$?
ELAPSED=$((EPOCHSECONDS - STARTED_AT))

if ((CLIENT_STATUS == 0)); then
    echo "the query timed out in the metadata request succeeded"
else
    echo "FAIL: the query timed out in the metadata request failed: $(cat "$ERROR_FILE")"
fi

if [[ -z "$RESULT" ]]; then
    echo "no rows were emitted after the cancellation"
else
    echo "FAIL: the cancelled query returned rows: $RESULT"
fi

if ((ELAPSED < 30)); then
    echo "returned soon after the timeout"
else
    echo "FAIL: the query kept running for $ELAPSED seconds"
fi

if (($(stat_count "GET /data") == 0)); then
    echo "the data was not requested after the cancellation"
else
    echo "FAIL: the data was requested after the cancellation"
fi

# 2. The timeout interrupts the retries of the first failover option. The query must not go on
# probing the next one - no requests to /fail2.
STARTED_AT=$EPOCHSECONDS
RESULT=$(run_interrupted_query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/fail1|http://127.0.0.1:$HTTP_PORT/fail2', 'CSV', 'x UInt64')" 2>"$ERROR_FILE")
CLIENT_STATUS=$?
ELAPSED=$((EPOCHSECONDS - STARTED_AT))

if ((CLIENT_STATUS == 0)); then
    echo "the query timed out in the failover probing succeeded"
else
    echo "FAIL: the query timed out in the failover probing failed: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

if ((ELAPSED < 30)); then
    echo "returned soon after the timeout"
else
    echo "FAIL: the query kept running for $ELAPSED seconds"
fi

if (($(stat_count "GET /fail2") + $(stat_count "HEAD /fail2") == 0)); then
    echo "the next failover option was not probed after the cancellation"
else
    echo "FAIL: the next failover option was probed after the cancellation"
fi

# 3. A query which is not interrupted keeps the old behavior: the failing HEAD request for the
# metadata is not an error, and the data is served.
if [[ $($CLICKHOUSE_CLIENT \
    --http_max_tries 2 \
    --http_retry_initial_backoff_ms 10 \
    --http_retry_max_backoff_ms 20 \
    --query "SELECT count() FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')") == 5 ]]; then
    echo "a query which is not interrupted reads the data despite the failing metadata request"
else
    echo "FAIL: a query which is not interrupted did not read the data"
fi
