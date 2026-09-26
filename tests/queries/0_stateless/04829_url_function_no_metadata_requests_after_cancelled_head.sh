#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a metadata `HEAD` request interrupted by a cancellation is not swallowed by the
# fallback for servers which do not support `HEAD`. `ReadWriteBufferFromHTTP::getFileInfo` treats a
# non-retriable 4xx response as "the server cannot answer this" and reports no metadata, so
# `StorageURLSource::initialize` used to complete as if the file simply had none - the request no
# one needs was interrupted for nothing. The cancellation must come out of every exit of the
# metadata request the same way: as the error of the interrupted read, which the source then
# discards or fails with depending on the kind of the cancellation, see
# `StorageURLSource::generate`. Here the `HEAD` is answered with 400 only after a soft
# `max_execution_time` (the `break` overflow mode) has cancelled the read: the query still
# succeeds with its (empty) partial result, makes no request after the cancellation, and the log
# records that the interrupted read was reported and its error discarded - not swallowed by the
# no-metadata fallback.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - HEAD /file withholds its 400 response - the "HEAD is not supported" fallback status - until the
#   test requests /release, after the cancellation has been delivered, so that no timing can
#   release the source early.
# - GET /file serves the data at once: only the cancellation, not a failure, stops the source from
#   requesting it.
# The server must serve requests in parallel: the test polls /stats and requests /release while
# HEAD /file is being served, and a single-threaded server would block them until the query has
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
        elif self.path == '/file':
            if head:
                release.wait(60)
                self.send_error(400)
            else:
                body = b'1\n2\n3\n4\n5\n'
                self.send_response(200)
                self.send_header('Content-Type', 'text/csv')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
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

QUERY_ID="${CLICKHOUSE_DATABASE}_cancelled_head"
ERROR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.err")

# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids: the log the test polls for would be left under a different
# query id.
$CLICKHOUSE_CLIENT \
    --max_execution_time 1 \
    --timeout_overflow_mode break \
    --http_make_head_request 1 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/file', 'CSV', 'x UInt64')" \
    2>"$ERROR_FILE" &
CLIENT_PID=$!

# The timeout is delivered to the source while it is blocked in the HEAD request: the source leaves
# a trace in the log, which the test waits for before releasing the response.
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
    echo "the cancellation was delivered while the source was blocked in the HEAD request"
else
    echo "FAIL: no cancellation reached the source"
fi

# Only now, with the cancellation delivered, answer the HEAD request the source is blocked in with
# the status which means "the server does not support HEAD". The source must not treat it as a file
# without metadata and go on requesting the size and the data.
curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((CLIENT_STATUS == 0)); then
    echo "the timed out query succeeded with its partial result"
else
    echo "FAIL: the timed out query failed with status $CLIENT_STATUS: $(cat "$ERROR_FILE")"
fi
rm -f "$ERROR_FILE"

if (($(stat_count "HEAD /file") == 1 && $(stat_count "GET /file") == 0)); then
    echo "no request was made after the cancelled HEAD"
else
    echo "FAIL: requests after the cancelled HEAD: $(curl -sS "http://127.0.0.1:$HTTP_PORT/stats")"
fi

# The cancelled HEAD must come out of the metadata request as the error of the interrupted read,
# which the source discards for the partial result - not be swallowed by the fallback for servers
# without HEAD support, completing the initialization as if the file had no metadata.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND message LIKE '%discarding the error%'") != 0 ]]; then
    echo "the error of the cancelled HEAD was reported and discarded for the partial result"
else
    echo "FAIL: the cancelled HEAD was swallowed as a file without metadata"
fi
