#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `http_read_buffer_pause_before_metadata_fallback`
# failpoint, which any concurrent query reading over HTTP would trip instead of this test's one.
#
# Test that a soft cancellation does not turn a metadata failure the query would have survived into
# a failure of the query. The fallbacks of the metadata helpers of `ReadWriteBufferFromHTTP` treat
# the failure of their `HEAD` request as an answer - the file comes out as one without the metadata -
# unless the request was interrupted by a cancellation. That distinction must be made by the mark
# the abandoned request leaves, not by the cancellation flag at the moment the fallback runs: when
# the `HEAD` has failed on its own before the cancellation arrived, a soft cancellation delivered
# while the failure is being unwound must leave the error swallowed, and the query must return its
# partial result instead of the stale metadata error. The window between the failure of the request
# and the fallback is a few instructions wide, so the test holds the source in it with a failpoint:
# the server tears down the HEAD request abruptly, the source pauses on the failpoint inside the
# fallback, the test cancels the client with SIGINT - `partial_result_on_first_cancel` makes that a
# soft cancellation, and its delivery is under the test's control, unlike a timer racing the query -
# and only then releases the failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /nohead answers a HEAD request by tearing the connection down without a response - the network
#   error the fallback would swallow - and would serve a small file to a GET request, which must
#   never arrive: the cancellation is delivered before the fallback returns.
# The server must serve requests in parallel: the test polls /stats while the query is being
# served, and a single-threaded server would block the polls behind the query's requests.
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
        elif self.path == '/nohead':
            if head:
                self.close_connection = True
                self.connection.close()
                return
            body = b'1\n2\n3\n'
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
# The failpoint must not stay armed after the test: a query of a later test would pause on it with
# no one left to release it.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT http_read_buffer_pause_before_metadata_fallback" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

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

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT http_read_buffer_pause_before_metadata_fallback"

QUERY_ID="${CLICKHOUSE_DATABASE}_stale_metadata_error"

# http_max_tries stops the retries of the torn-down HEAD request: the request must fail once and
# for all before the cancellation, not be interrupted by it on a retry.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids: the cancel of this client would not reach the source the same
# way, and the log the test waits for would be left under a different query id.
$CLICKHOUSE_CLIENT \
    --partial_result_on_first_cancel 1 \
    --http_make_head_request 1 \
    --http_max_tries 1 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/nohead', 'CSV', 'x UInt64')" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

# Wait until the query is registered - so that the SIGINT below cancels the query instead of a
# client that has not started it yet - and its metadata probe has sent the HEAD request, after
# which the source pauses on the failpoint inside the fallback of the failed request. The HEAD has
# failed with no cancellation in sight: the fallback must swallow its error no matter what arrives
# next.
for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done
for _ in {1..300}; do
    (($(stat_count "HEAD /nohead") > 0)) && break
    sleep 0.1
done

# The soft cancellation: the client asks for its partial result, the query keeps running.
kill -SIGINT $CLIENT_PID

# Wait until the cancellation has reached the source, which leaves a trace in the log.
DELIVERED=0
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read has been cancelled%'") != 0 ]]; then
        DELIVERED=1
        break
    fi
    sleep 0.1
done

# Only now, with the cancellation delivered, release the source from the fallback. It must swallow
# the error of the request the cancellation did not interrupt and end the stream instead of
# reporting it.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT http_read_buffer_pause_before_metadata_fallback"

wait $CLIENT_PID
CLIENT_STATUS=$?

if ((DELIVERED == 1)); then
    echo "the cancellation was delivered while the source was paused inside the metadata fallback"
else
    echo "FAIL: the cancellation was not delivered while the source was paused inside the metadata fallback"
fi

# The query is cancelled softly: it returns what it has read - nothing - and the stale error of
# the request the cancellation did not interrupt stays swallowed.
if ((CLIENT_STATUS == 0)); then
    echo "the query succeeded"
else
    echo "FAIL: the query failed with the status $CLIENT_STATUS"
fi

if (($(stat_count "GET /nohead") == 0)); then
    echo "the data was not requested after the cancellation"
else
    echo "FAIL: the data was requested after the cancellation"
fi
