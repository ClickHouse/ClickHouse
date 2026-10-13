#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_before_empty_file_probe` failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ReadBuffer::eof` starts a lazy HTTP GET when it is called to skip empty URL files. Once the
# buffer has been selected, cancelling the query must prevent that GET. The failpoint holds the
# first request after the source's outer cancellation guard, then a controlled soft cancellation
# is delivered with SIGINT.

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -u -c "
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

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
        elif self.path == '/data':
            body = b'1\\n2\\n3\\n'
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

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_request_attempt" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

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

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_before_request_attempt"

QUERY_ID="${CLICKHOUSE_DATABASE}_no_initial_get_after_cancel"
$CLICKHOUSE_CLIENT \
    --partial_result_on_first_cancel 1 \
    --parallel_replicas_for_cluster_engines 0 \
    --engine_url_skip_empty_files 1 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

# Wait until the first request is about to begin after all outer cancellation guards.
wait_for_pause storage_url_pause_before_request_attempt

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

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_request_attempt"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((DELIVERED == 1)); then
    echo "the cancellation was delivered after URI selection"
else
    echo "FAIL: the cancellation was not delivered after URI selection"
fi

if ((CLIENT_STATUS == 0)); then
    echo "the query succeeded"
else
    echo "FAIL: the query failed with the status $CLIENT_STATUS"
fi

if (($(stat_count "GET /data") == 0)); then
    echo "the data request was not started after the cancellation"
else
    echo "FAIL: the data request was started after the cancellation"
fi
