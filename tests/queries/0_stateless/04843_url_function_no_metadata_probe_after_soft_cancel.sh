#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a cancellation which arrives after `StorageURLSource` has chosen the URI to read from,
# but before it has requested the metadata of the file - the modification time and the size, which
# take a HEAD request when the response of the chosen URI does not carry them - stops the
# initialization instead of letting it probe the metadata of a file no one is left to read, see
# `StorageURLSource::initialize`. The cancellation lands in exactly that window: the query reads
# from two failover options, so the request to the first one is made while the URI is being chosen,
# the server withholds its response until the cancellation - a soft `max_execution_time` timeout
# with the `break` overflow mode - has been delivered, and the response carries no `Content-Length`,
# so the metadata could only come from a HEAD request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /held withholds its response until the test requests /release - after the cancellation has been
#   delivered - and then serves a small file without a `Content-Length`, so that the metadata of
#   the file could only come from a later HEAD request.
# - /other is the next failover option, which must not be touched at all.
# - /reset starts the next attempt from scratch, for the case when the cancellation preempts the
#   first request, see below.
# The server must serve requests in parallel: the test polls /stats and requests /release while
# /held is being served, and a single-threaded server would block them until the query has already
# finished, too late to cancel it.
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
        elif self.path == '/reset':
            release.clear()
            counts.clear()
            self.send_response(200)
            self.end_headers()
        elif self.path == '/held':
            release.wait(60)
            try:
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv')
                self.end_headers()
                if not head:
                    self.wfile.write(b'1\n2\n3\n')
            except BrokenPipeError:
                pass  # The reader may be gone by the time the response is released.
        elif self.path == '/other':
            body = b'4\n5\n6\n'
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

# The cancellation must land while the request to /held is in flight, that is after the URI choice
# has passed its own cancellation checks. When the server is slow to start the query - under a
# sanitizer, for example - the soft timeout can fire before the first request has been made, and
# the read stops during the URI choice instead of after it: nothing to assert then, so retry.
for attempt in {1..10}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/reset" -o /dev/null

    QUERY_ID="${CLICKHOUSE_DATABASE}_no_metadata_probe_$attempt"

    # parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
    # queries with their own query ids: the log the test waits for would be left under a different
    # query id.
    $CLICKHOUSE_CLIENT \
        --max_execution_time 1 \
        --timeout_overflow_mode 'break' \
        --http_make_head_request 1 \
        --parallel_replicas_for_cluster_engines 0 \
        --query_id "$QUERY_ID" \
        --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/held|http://127.0.0.1:$HTTP_PORT/other', 'CSV', 'x UInt64')" \
        >/dev/null 2>/dev/null &
    CLIENT_PID=$!

    # Wait until the source is inside the request to the first option.
    for _ in {1..100}; do
        (($(stat_count "GET /held") > 0)) && break
        sleep 0.1
    done

    # Wait until the soft timeout has cancelled the source, which leaves a trace in the log.
    DELIVERED=0
    for _ in {1..120}; do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
        if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read has been cancelled%'") != 0 ]]; then
            DELIVERED=1
            break
        fi
        sleep 0.1
    done

    # Only now, with the cancellation delivered, let the server answer the request the source is
    # blocked in. The source gets its file, and must end the stream instead of requesting the
    # metadata no one needs.
    curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

    wait $CLIENT_PID
    CLIENT_STATUS=$?

    ((DELIVERED == 1)) && (($(stat_count "GET /held") > 0)) && break
done

if ((DELIVERED == 1)) && (($(stat_count "GET /held") > 0)); then
    echo "the cancellation was delivered while the source was blocked in the request"
else
    echo "FAIL: the cancellation was not delivered while the source was blocked in the request"
fi

# The query is not killed by the soft timeout: it returns what it has read - nothing.
if ((CLIENT_STATUS == 0)); then
    echo "the query succeeded"
else
    echo "FAIL: the query failed with the status $CLIENT_STATUS"
fi

if (($(stat_count "HEAD /held") == 0)); then
    echo "the metadata of the file was not probed after the cancellation"
else
    echo "FAIL: the metadata of the file was probed after the cancellation"
fi

if (($(stat_count "GET /other") + $(stat_count "HEAD /other") == 0)); then
    echo "the next option was not probed after the cancellation"
else
    echo "FAIL: the next option was probed after the cancellation"
fi
