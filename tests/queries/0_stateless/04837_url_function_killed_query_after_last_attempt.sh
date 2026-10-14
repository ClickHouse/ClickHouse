#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a query killed while the last request attempt is in flight is reported as cancelled
# rather than with the network error of that attempt. `ReadWriteBufferFromHTTP::doWithRetries` asked
# the query status about the query only where it decided to wait and try again, so the attempt after
# which there is nothing left to retry reported the error of the server instead of the cancellation.
# The readers which pass a cancellation flag guard themselves with it, but the ones which do not -
# the `web` disk, the HTTP dictionaries, the data lake catalogs, and the schema inference of the
# `url` table function, which this test uses - had only that check.
#
# The server here withholds its `503` response until the test has killed the query, so the read is
# interrupted by the kill in the only attempt it is allowed to make: the query must fail with the
# cancellation, and the same query which is not killed must still report the `503` of the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# The server binds to the port 0 and reports the port the kernel gave it, so that it cannot collide
# with anything else running in parallel. GET /held withholds its `503` until the test requests
# /release, after the query has been killed, so that no timing can let the request finish early.
# The server must serve requests in parallel: the test polls /stats and requests /release while
# /held is being served, and a single-threaded server would block them until the query has already
# finished, too late to kill it.
python3 -u -c "
import json
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

counts = {}
release = threading.Event()

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
        elif self.path == '/release':
            release.set()
            self.send_response(200)
            self.end_headers()
        elif self.path == '/held':
            release.wait(60)
            self.send_error(503)
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

QUERY_ID="${CLICKHOUSE_DATABASE}_killed_last_attempt"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# The structure is left to the schema inference on purpose: it reads the URL with no cancellation
# flag, so the query status is the only thing which can tell the read that the query is gone.
# One attempt only, so that the read is interrupted where the retrying ends.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids, which the kill of this query id would not reach.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/held', 'TSV')" \
    2>"$ERROR_FILE" &
CLIENT_PID=$!

# Kill the query while it is blocked in the request the server withholds.
for _ in {1..600}; do
    (($(stat_count "GET /held") != 0)) && break
    sleep 0.1
done

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
    echo "the query was killed while it was blocked in its only request attempt"
else
    echo "FAIL: the query was not killed while it was blocked"
fi

# Only now, with the query killed, let the server answer the request with its error.
curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS != 0)) && grep -q "QUERY_WAS_CANCELLED" "$ERROR_FILE"; then
    echo "the killed query is reported as cancelled"
else
    echo "FAIL: the killed query is reported with status $CLIENT_STATUS: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

# The same query which is not killed still reports the error of the server as before. The response
# is no longer withheld, so it comes back at once.
$CLICKHOUSE_CLIENT \
    --http_max_tries 1 \
    --http_make_head_request 0 \
    --parallel_replicas_for_cluster_engines 0 \
    --query "SELECT * FROM url('http://127.0.0.1:$HTTP_PORT/held', 'TSV')" 2>&1 \
    | grep -c -m1 "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"
