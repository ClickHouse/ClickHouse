#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_between_metadata_probes` failpoint,
# which any concurrent query reading over HTTP would trip instead of this test's one.
#
# Test that a cancellation which arrives between the two metadata probes of `StorageURLSource::initialize`
# - the probe of the modification time and the probe of the file size - does not let the second probe
# send a fresh HEAD request. The two probes share the metadata of one HEAD request, but when that request
# has failed with a network error before the cancellation arrived, the first probe has nothing to remember,
# and without a check in between the second probe would request the metadata of a file no one is left
# to read. The window between the probes is a few instructions wide, so the test holds the source in it
# with a failpoint: the server tears down the HEAD request abruptly, the source pauses on the failpoint,
# the test delivers a soft `max_execution_time` timeout with the `break` overflow mode, and only then
# releases the failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can assert which requests were
# made after the cancellation. It binds to the port 0 and reports the port the kernel gave it, so
# that it cannot collide with anything else running in parallel.
#
# - /nohead answers a HEAD request by tearing the connection down without a response - the network
#   error which leaves the first metadata probe with nothing to remember - and would serve a small
#   file to a GET request, which must never arrive.
# - /reset starts the next attempt from scratch, for the case when the cancellation preempts the
#   first request, see below.
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
        elif self.path == '/reset':
            counts.clear()
            self.send_response(200)
            self.end_headers()
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
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_between_metadata_probes" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

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

# The HEAD request must have failed before the cancellation arrived - a request interrupted by the
# cancellation is reported from the probe itself and never reaches the window between the probes,
# which is a different, already covered path. When the server is slow to start the query - under
# a sanitizer, for example - the soft timeout can fire first: nothing to assert then, so retry.
for attempt in {1..10}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/reset" -o /dev/null
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_between_metadata_probes"

    QUERY_ID="${CLICKHOUSE_DATABASE}_no_second_metadata_probe_$attempt"

    # http_max_tries stops the retries of the torn-down HEAD request: the first probe must fail
    # while the query is still alive.
    # parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
    # queries with their own query ids: the log the test waits for would be left under a different
    # query id.
    $CLICKHOUSE_CLIENT \
        --max_execution_time 1 \
        --timeout_overflow_mode 'break' \
        --http_make_head_request 1 \
        --http_max_tries 1 \
        --parallel_replicas_for_cluster_engines 0 \
        --query_id "$QUERY_ID" \
        --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/nohead', 'CSV', 'x UInt64')" \
        >/dev/null 2>/dev/null &
    CLIENT_PID=$!

    # Wait until the first probe has sent its HEAD request, after which the source pauses on the
    # failpoint between the probes.
    for _ in {1..100}; do
        (($(stat_count "HEAD /nohead") > 0)) && break
        sleep 0.1
    done

    # The probe must have run before the cancellation, see above.
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    PREEMPTED=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read has been cancelled%'")

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

    # Only now, with the cancellation delivered, release the source from the window between the
    # probes. It must end the stream instead of sending another HEAD request.
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_between_metadata_probes"

    wait $CLIENT_PID
    CLIENT_STATUS=$?

    ((DELIVERED == 1)) && [[ "$PREEMPTED" == 0 ]] && (($(stat_count "HEAD /nohead") > 0)) && break
done

if ((DELIVERED == 1)) && [[ "$PREEMPTED" == 0 ]] && (($(stat_count "HEAD /nohead") > 0)); then
    echo "the cancellation was delivered while the source was paused between the metadata probes"
else
    echo "FAIL: the cancellation was not delivered while the source was paused between the metadata probes"
fi

# The query is not killed by the soft timeout: it returns what it has read - nothing.
if ((CLIENT_STATUS == 0)); then
    echo "the query succeeded"
else
    echo "FAIL: the query failed with the status $CLIENT_STATUS"
fi

if (($(stat_count "HEAD /nohead") == 1)); then
    echo "the metadata of the file was not probed again after the cancellation"
else
    echo "FAIL: the metadata of the file was probed again after the cancellation"
fi

if (($(stat_count "GET /nohead") == 0)); then
    echo "the data was not requested after the cancellation"
else
    echo "FAIL: the data was requested after the cancellation"
fi
