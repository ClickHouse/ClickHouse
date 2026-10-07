#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a query over the `url` function stops right after it is killed, even when it is retrying
# an HTTP request, instead of waiting until all the retry attempts are exhausted.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")
LOG_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.log")

# A server which always answers with a retriable error, so that the query gets stuck in the retry loop
# of `ReadWriteBufferFromHTTP`. It binds to the port 0 and reports the port the kernel gave it, so that
# it cannot collide with anything else running in parallel.
python3 -u -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def respond(self):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_error(503)

    do_HEAD = respond
    do_GET = respond

    def log_message(self, *args):
        pass

server = HTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$LOG_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

QUERY="SELECT count() FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64') FORMAT Null"
QUERY_ID="04615_${CLICKHOUSE_DATABASE}_$RANDOM"

# 30 attempts with a backoff of 1 to 2 seconds, for both the HEAD and the GET request: minutes of
# retrying if a cancellation is noticed only after the retries are over.
$CLICKHOUSE_CLIENT \
    --http_max_tries 30 \
    --http_retry_initial_backoff_ms 1000 \
    --http_retry_max_backoff_ms 2000 \
    --query_id "$QUERY_ID" \
    --query "$QUERY" >/dev/null 2>"$LOG_FILE" &
CLIENT_PID=$!

wait_for_query_to_start "$QUERY_ID"

KILLED_AT=$EPOCHSECONDS
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" >/dev/null
wait $CLIENT_PID
CLIENT_STATUS=$?
ELAPSED=$((EPOCHSECONDS - KILLED_AT))

if ((ELAPSED < 30)); then
    echo "stopped soon after the kill"
else
    echo "FAIL: the query kept running for $ELAPSED seconds after the kill"
fi

if ((CLIENT_STATUS != 0)); then
    echo "reported as an error"
else
    echo "FAIL: the killed query succeeded"
fi

# A query which is not cancelled must still report the error of the server as it did before.
if $CLICKHOUSE_CLIENT \
    --http_max_tries 2 \
    --http_retry_initial_backoff_ms 10 \
    --http_retry_max_backoff_ms 20 \
    --query "$QUERY" 2>&1 | grep -q -F "HTTP status code: 503"; then
    echo "the error of the server is reported when the query is not killed"
else
    echo "FAIL: the error of the server is not reported"
fi
