#!/usr/bin/env bash
# Tags: no-fasttest
# A `KILL QUERY` which arrives after `StorageURLSource` has already ended its stream must still fail
# the query: the source signals the end of the data with EOF - after a cancellation as well as after
# the last byte of the response - and it is the executor which turns the kill into the cancellation
# error. `PipelineExecutor::finalizeExecution` calls `checkTimeLimit` before it looks at its own
# execution status, and `QueryStatus::checkTimeLimit` throws for a killed query, so a query cannot
# succeed with a partial result because the kill landed after the last check inside the source.
# Here the whole response has been read - the request counter proves it - before the query is
# killed while the rest of the pipeline is still running.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can wait until the source has
# read the whole file before it kills the query. It binds to the port 0 and reports the port the
# kernel gave it, so that it cannot collide with anything else running in parallel.
python3 -u -c "
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

counts = {}

class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        method = 'HEAD' if head else 'GET'
        if self.path == '/stats':
            body = json.dumps(counts).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)
            return

        counts[f'{method} {self.path}'] = counts.get(f'{method} {self.path}', 0) + 1
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/data':
            body = b'1\n2\n3\n'
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

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
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

QUERY_ID="${CLICKHOUSE_DATABASE}_kill_after_source_finished"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# The aggregation consumes the whole file, so the source is finished by the time the last request is
# counted; `sleepEachRow` then holds the pipeline while the query is killed. The sleep is long, and
# the limit on it is raised accordingly, because everything the test has to do before the `KILL` -
# two polling loops, each of them running `clickhouse-client` - takes seconds on a loaded sanitizer
# runner, and the query must still be running by then. It does not make the test slow: `sleep`
# wakes up every second to call `QueryStatus::checkTimeLimit`, which throws for a killed query, so
# the query ends within a second of the `KILL`.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids, and the kill of this query id would not reach the source.
$CLICKHOUSE_CLIENT \
    --parallel_replicas_for_cluster_engines 0 \
    --function_sleep_max_microseconds_per_block 60000000 \
    --query_id "$QUERY_ID" \
    --query "SELECT sleepEachRow(60) FROM (SELECT count() FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64'))" \
    >/dev/null 2>"$ERROR_FILE" &
CLIENT_PID=$!

for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done

# The response has been read in full: the source has nothing left to read and has ended its stream.
for _ in {1..300}; do
    (($(stat_count "GET /data") > 0)) && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC" >/dev/null

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q -F "QUERY_WAS_CANCELLED" "$ERROR_FILE"; then
    echo "the killed query failed with the cancellation error"
else
    echo "FAIL: the killed query returned status $CLIENT_STATUS with: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

if (($(stat_count "GET /data") == 1)); then
    echo "the file was read once, before the kill"
else
    echo "FAIL: the file was read $(stat_count "GET /data") times"
fi
