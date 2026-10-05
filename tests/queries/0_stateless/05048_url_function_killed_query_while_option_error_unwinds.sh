#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_before_handling_option_error`
# failpoint, which any concurrent url query whose request fails would trip instead of this test's
# one.
#
# Test that a query killed while the error of its only failover option unwinds is reported as
# cancelled rather than with that error. `ReadWriteBufferFromHTTP::doWithRetries` asks the query
# status about the query at its terminal exit, but a kill or a hard timeout can land after that
# check, while the error unwinds to the failover loop of
# `StorageURLSource::getFirstAvailableURIAndReadBuffer` - and for the readers which pass no
# cancellation flag, such as the schema inference of the `url` table function which this test
# uses, nothing on that path asked the query status again: a lone option was rethrown as the
# stale HTTP error of the request.
#
# The window is a few instructions wide, so the test holds the source in it with a failpoint:
# the only attempt of the only option fails at once, the unwinding pauses right before the error
# is handled, the query is killed, and only then the source is released. The killed query must
# fail with the cancellation, and the same query which is not killed must still report the error
# of the server as before.

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

QUERY_ID="${CLICKHOUSE_DATABASE}_killed_while_error_unwinds"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# The structure is left to the schema inference on purpose: it reads the URL with no cancellation
# flag, so the query status is the only thing which can tell the read that the query is gone.
# One attempt only, so that the error of the request unwinds where the retrying ends.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids, which the kill of this query id would not reach.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/bad', 'TSV')" \
    2>"$ERROR_FILE" &
CLIENT_PID=$!

# The only attempt of the only option has failed, and its error is held right before it is
# handled: exactly the window in which the kill below used to be masked by it.
wait_for_pause storage_url_pause_before_handling_option_error
echo "the error of the only option was caught before it was handled"

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' ASYNC" > /dev/null

KILLED=0
for _ in {1..600}; do
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID' AND is_cancelled") != 0 ]]; then
        KILLED=1
        break
    fi
    sleep 0.1
done
if ((KILLED == 1)); then
    echo "the query was killed while the error was held"
else
    echo "FAIL: the query was not killed while the error was held"
fi

# Only now, with the query killed, let the error of the request unwind.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_handling_option_error"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q "QUERY_WAS_CANCELLED" "$ERROR_FILE"; then
    echo "the killed query is reported as cancelled"
else
    echo "FAIL: the killed query is reported with status $CLIENT_STATUS: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

# The same query which is not killed still reports the error of the server as before.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/bad', 'TSV')" 2>&1 \
    | grep -c -m1 "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"
