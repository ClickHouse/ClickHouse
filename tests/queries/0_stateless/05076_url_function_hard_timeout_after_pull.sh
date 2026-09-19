#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_after_pull` failpoint, which any
# concurrent url query would trip instead of this test's one.
#
# Test that a query whose `max_execution_time` runs out with the `throw` overflow mode after
# `StorageURLSource::generate` has pulled a chunk fails with the timeout and does not emit that
# chunk. It is the hard-timeout sibling of 04904, which cancels the query in the same window: there
# the source is told about the cancellation, here the deadline of the query has merely passed -
# the executor polls the time limit only before a step, and `CurrentThread::checkIfNotCancelled`
# sees the timeout only once `CancellationChecker` has turned it into a kill - so the source has to
# ask the query status about the time limit itself before it hands the chunk over.
#
# The window is a few instructions wide, so the test holds the source in it with a failpoint: the
# chunk is pulled, the source pauses right before it checks the cancellation, the deadline of the
# query passes, and only then the source is released.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")
OUTPUT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.output")
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# The server binds to the port 0 and reports the port the kernel gave it, so that it cannot
# collide with anything else running in parallel. It serves one row at once: the sequencing of the
# test is done by the failpoint, not by the server.
python3 -u -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'1\\n'
        self.send_response(200)
        self.send_header('Content-Type', 'text/csv')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Content-Length', '2')
        self.end_headers()

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_after_pull" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$OUTPUT_FILE" "$ERROR_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

# The wait for a failpoint to pause has no timeout of its own: bound it, so that a sequence which
# did not happen fails the test with a diagnostic instead of hanging it - and leaving the
# server-wide failpoint armed for the tests which run after it.
wait_for_pause()
{
    if ! timeout 300 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE"; then
        echo "FAIL: the failpoint $1 was not reached"
        exit 1
    fi
}

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_after_pull"

QUERY_ID="${CLICKHOUSE_DATABASE}_hard_timeout_after_pull"

# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own deadlines and process list entries.
$CLICKHOUSE_CLIENT \
    --parallel_replicas_for_cluster_engines 0 \
    --max_execution_time 3 \
    --timeout_overflow_mode throw \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >"$OUTPUT_FILE" 2>"$ERROR_FILE" &
CLIENT_PID=$!

# The chunk has been pulled and is held right before the source checks whether anyone still
# needs it: exactly the window in which the deadline below used to go unnoticed.
wait_for_pause storage_url_pause_after_pull
echo "the chunk was pulled before the deadline"

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
    echo "the execution time limit was exceeded while the chunk was held"
else
    echo "FAIL: the execution time limit was not exceeded while the chunk was held"
fi

# Only now, with the deadline of the query passed, let the source go on with the chunk.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_after_pull"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q "TIMEOUT_EXCEEDED" "$ERROR_FILE"; then
    echo "the timed out query is reported as timed out"
else
    echo "FAIL: the timed out query is reported with status $CLIENT_STATUS: $(cat "$ERROR_FILE")"
fi

if [[ ! -s "$OUTPUT_FILE" ]]; then
    echo "the chunk pulled before the deadline was not emitted"
else
    echo "FAIL: the chunk pulled before the deadline was emitted: $(cat "$OUTPUT_FILE")"
fi

# The same query which is given enough time still returns the row as before.
$CLICKHOUSE_CLIENT \
    --parallel_replicas_for_cluster_engines 0 \
    --max_execution_time 300 \
    --timeout_overflow_mode throw \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')"
