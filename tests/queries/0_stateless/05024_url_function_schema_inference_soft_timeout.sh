#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_before_read_buffer_creation` failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The schema inference reads a `url` with no `StorageURLSource` behind it, so its reads have no
# cancellation token. The soft timeout, which the choosing of the failover option latches into that
# token for the source, must not be latched into nothing: hold the inference precisely before the
# creation of the first buffer, let `max_execution_time` expire while it waits, and release it into
# the choosing of the next option, where the latch is.

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
        elif self.path == '/failed':
            self.send_error(503)
        elif self.path == '/data':
            body = b'1\\n2\\n3\\n'
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
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_read_buffer_creation" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

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

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_before_read_buffer_creation"

QUERY_ID="${CLICKHOUSE_DATABASE}_schema_inference_soft_timeout"
$CLICKHOUSE_CLIENT \
    --parallel_replicas_for_cluster_engines 0 \
    --max_execution_time 1 \
    --timeout_overflow_mode break \
    --query_id "$QUERY_ID" \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/failed|http://127.0.0.1:$HTTP_PORT/data', 'CSV')" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

wait_for_pause storage_url_pause_before_read_buffer_creation

# Ensure that `max_execution_time` expires while the inference is held precisely before `create`.
sleep 2

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_read_buffer_creation"

# Whether the query ends with the rows of the second option or with nothing is up to the timing of
# the timeout; what it must not do is take the server down.
wait $CLIENT_PID
echo "the query ended"

$CLICKHOUSE_CLIENT --query "SELECT 'the server is alive'"
