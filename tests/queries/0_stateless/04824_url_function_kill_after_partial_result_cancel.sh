#!/usr/bin/env bash
# Tags: no-fasttest, no-sanitizers-lsan
# Test that a hard cancellation which arrives after a soft one is not treated as soft. The first
# cancel of a client with `partial_result_on_first_cancel` asks the sources to stop reading
# (`CancelReason::PartialResult`): the query must still succeed with what it has already read, so
# `StorageURLSource` discards the error of the read it interrupts. A `KILL QUERY` after that
# upgrades the cancellation (see `ExecutingGraph::cancel`), the query fails, and the source must
# fail with the real cancellation error instead of discarding it - the killed query must never end
# its stream as if it completed. The kill lands while the source is blocked in a request that the
# cancellation cannot interrupt (a slowly served empty file), so the upgraded, no longer soft
# cancellation is what the source finds when the request completes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /empty_slow serves an empty file after a delay, long enough for both cancellations to arrive
#   while it is being served.
# - /data serves the data at once.
python3 -u -c "
import json
import time
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
        elif self.path == '/empty_slow':
            time.sleep(5)
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', '0')
            self.end_headers()
        elif self.path == '/data':
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

stat_count()
{
    curl -sS "http://127.0.0.1:$HTTP_PORT/stats" | python3 -c "import sys, json; print(json.load(sys.stdin).get('$1', 0))"
}

QUERY_ID="${CLICKHOUSE_DATABASE}_kill_after_partial_result"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

$CLICKHOUSE_CLIENT \
    --partial_result_on_first_cancel 1 \
    --engine_url_skip_empty_files 1 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/empty_slow|http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >/dev/null 2>"$ERROR_FILE" &
CLIENT_PID=$!

# Wait until the query is registered and its first request is being served, so that both
# cancellations land while the source is inside the request.
for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done
for _ in {1..300}; do
    (($(stat_count "GET /empty_slow") + $(stat_count "HEAD /empty_slow") > 0)) && break
    sleep 0.1
done

# The first cancel: the client asks for its partial result, the query keeps running.
kill -SIGINT $CLIENT_PID
sleep 0.5

# The second, hard cancellation: the query is killed and must fail.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" >/dev/null

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q -F "QUERY_WAS_CANCELLED" "$ERROR_FILE"; then
    echo "the killed query failed with the cancellation error"
else
    echo "FAIL: the killed query returned status $CLIENT_STATUS with: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

if (($(stat_count "GET /data") + $(stat_count "HEAD /data") == 0)); then
    echo "the next failover option was not probed after the kill"
else
    echo "FAIL: the next failover option was probed after the kill"
fi

# The source must not have taken the partial-result path after the kill: the cancellation error of
# the killed query is not discarded but fails the query, see above.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND message LIKE '%discarding the error%'") == 0 ]]; then
    echo "the cancellation error of the killed query was not discarded"
else
    echo "FAIL: the killed query discarded the cancellation error as if its result were partial"
fi
