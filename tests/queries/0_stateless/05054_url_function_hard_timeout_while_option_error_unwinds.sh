#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_before_handling_option_error`
# failpoint, which any concurrent url query whose request fails would trip instead of this test's
# one.
#
# Test that a query whose `max_execution_time` runs out with the `throw` overflow mode while the
# error of its only failover option unwinds is reported as the timeout rather than with that
# error. It is the sibling of 05048, which kills the query in the same window: there the query
# status has already been told that the query is gone, here the deadline of the query has merely
# passed, so the failover loop of `StorageURLSource::getFirstAvailableURIAndReadBuffer` has to ask
# the query status about the time limit itself - for the readers which pass no cancellation flag,
# such as the schema inference of the `url` table function which this test uses, nothing else on
# that path does it once `ReadWriteBufferFromHTTP::doWithRetries` has made its terminal check.
#
# The window is a few instructions wide, so the test holds the source in it with a failpoint: the
# only attempt of the only option fails at once, the unwinding pauses right before the error is
# handled, the deadline of the query passes, and only then the source is released.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# The server binds to the port 0 and reports the port the kernel gave it, so that it cannot
# collide with anything else running in parallel. It answers every request with a 503 at once:
# the sequencing of the test is done by the failpoint, not by the server.
python3 -u -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        else:
            self.send_error(503)

    def do_HEAD(self):
        self.respond(head=True)

    def do_GET(self):
        self.respond(head=False)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_handling_option_error" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_before_handling_option_error"

# The waits for a failpoint to pause have no timeout of their own: bound them, so that a sequence
# which did not happen fails the test with a diagnostic instead of hanging it - and leaving the
# failpoint armed for the tests which run after it.
wait_for_pause()
{
    if ! timeout 300 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE"; then
        echo "FAIL: the failpoint $1 was not reached"
        exit 1
    fi
}

QUERY_ID="${CLICKHOUSE_DATABASE}_timed_out_while_error_unwinds"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# The structure is left to the schema inference on purpose: it reads the URL with no cancellation
# flag, so the query status is the only thing which can tell the read that the query is over.
# One attempt only, so that the error of the request unwinds where the retrying ends.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own deadlines and process list entries.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --max_execution_time 3 \
    --timeout_overflow_mode throw \
    --query_id "$QUERY_ID" \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/bad', 'TSV')" \
    2>"$ERROR_FILE" &
CLIENT_PID=$!

# The only attempt of the only option has failed, and its error is held right before it is
# handled: exactly the window in which the deadline below used to be masked by it.
wait_for_pause storage_url_pause_before_handling_option_error
echo "the error of the only option was caught before it was handled"

# The query keeps waiting on the failpoint while its deadline passes.
EXPIRED=0
for _ in {1..600}; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND (is_cancelled OR elapsed > 4)") != 0 ]]; then
        EXPIRED=1
        break
    fi
    sleep 0.1
done
if ((EXPIRED == 1)); then
    echo "the execution time limit was exceeded while the error was held"
else
    echo "FAIL: the execution time limit was not exceeded while the error was held"
fi

# Only now, with the deadline of the query passed, let the error of the request unwind.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_handling_option_error"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q "TIMEOUT_EXCEEDED" "$ERROR_FILE"; then
    echo "the timed out query is reported as timed out"
else
    echo "FAIL: the timed out query is reported with status $CLIENT_STATUS: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

# The same query which is given enough time still reports the error of the server as before.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --max_execution_time 300 \
    --timeout_overflow_mode throw \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/bad', 'TSV')" 2>&1 \
    | grep -c -m1 "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"
