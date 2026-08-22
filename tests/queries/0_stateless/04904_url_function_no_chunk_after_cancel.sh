#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_after_pull` failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A cancellation delivered after `pull` has produced a chunk must prevent that chunk from reaching
# `ISource::prepare`, which pushes it before noticing the cancellation.

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")
OUTPUT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.output")

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
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_after_pull" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$OUTPUT_FILE"' EXIT

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

QUERY_ID="${CLICKHOUSE_DATABASE}_no_chunk_after_cancel"
$CLICKHOUSE_CLIENT \
    --partial_result_on_first_cancel 1 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >"$OUTPUT_FILE" 2>/dev/null &
CLIENT_PID=$!

wait_for_pause storage_url_pause_after_pull
kill -SIGINT $CLIENT_PID

DELIVERED=0
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read has been cancelled%'") != 0 ]]; then
        DELIVERED=1
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_after_pull"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS == 0)); then
    echo "the query succeeded"
else
    echo "FAIL: the query failed with the status $CLIENT_STATUS"
fi

if ((DELIVERED == 1)); then
    echo "the cancellation was delivered after the chunk was pulled"
else
    echo "FAIL: the cancellation was not delivered after the chunk was pulled"
fi

if [[ ! -s "$OUTPUT_FILE" ]]; then
    echo "the cancelled chunk was not emitted"
else
    echo "FAIL: the cancelled chunk was emitted"
fi
