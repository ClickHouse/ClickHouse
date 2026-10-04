#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a hard teardown of the pipeline which does not kill the query - here the client
# disconnecting - stops `StorageURLSource` from probing the next failover option. Such a
# cancellation reaches the source while no request is in flight to interrupt: the first option is an
# empty file (skipped with `engine_url_skip_empty_files`), and the cancellation arrives while it is
# being served, so the source finds it only between the options. It must not start the request to
# the next option - no one is left who needs the data - even though that request would succeed, see
# `StorageURLSource::getFirstAvailableURIAndReadBuffer`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /empty_held serves an empty file, withholding the response until the test requests /release -
#   after the cancellation of the disconnected query has been delivered - so that no timing can
#   release the source early.
# - /data serves the data at once.
# The server must serve requests in parallel: the test polls /stats and requests /release while
# /empty_held is being served, and a single-threaded server would block them until the query has
# already finished, too late to cancel it.
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
        elif self.path == '/empty_held':
            release.wait(60)
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

QUERY_ID="${CLICKHOUSE_DATABASE}_no_next_option_after_disconnect"

# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids: the log the test asserts on would be left under a different
# query id.
$CLICKHOUSE_CLIENT \
    --engine_url_skip_empty_files 1 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/empty_held|http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

# Wait until the query is registered and its first request is being served, so that the
# cancellation lands while the source is inside the request.
for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done
for _ in {1..300}; do
    (($(stat_count "GET /empty_held") + $(stat_count "HEAD /empty_held") > 0)) && break
    sleep 0.1
done

# The client disconnects without canceling the query first. The server notices, tears the pipeline
# down, and the cancellation - a hard one which does not kill the query - reaches the source, which
# leaves a trace in the log.
kill -9 $CLIENT_PID
wait $CLIENT_PID 2>/dev/null

DELIVERED=0
for _ in {1..120}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read has been cancelled%'") != 0 ]]; then
        DELIVERED=1
        break
    fi
    sleep 0.1
done
if ((DELIVERED == 1)); then
    echo "the cancellation was delivered while the source was blocked in the request"
else
    echo "FAIL: no cancellation reached the source after the client disconnected"
fi

# Only now, with the cancellation delivered, let the server answer the request the source is
# blocked in. The source finds the empty file, and must end the stream instead of probing the next
# option.
curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 0 ]] && break
    sleep 0.1
done

if (($(stat_count "GET /data") + $(stat_count "HEAD /data") == 0)); then
    echo "the next option was not probed after the client disconnected"
else
    echo "FAIL: the next option was probed after the client disconnected"
fi
