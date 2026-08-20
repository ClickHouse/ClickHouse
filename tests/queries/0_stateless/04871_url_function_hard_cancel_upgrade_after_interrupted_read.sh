#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-parallel: the test pauses the server-wide `storage_url_pause_before_retry_attempt` and
# `storage_url_pause_before_handling_interrupted_read_error` failpoints, which any concurrent url
# query whose read fails would trip instead of this test's one.
#
# Test that an exception built under a soft cancellation does not outrun the upgrade of that
# cancellation to a hard one. `ExecutingGraph::cancel` upgrades `PartialResult` to the reason of a
# later hard cancellation, but the error of the interrupted read may already be in flight - thrown
# by `ReadWriteBufferFromHTTP::doWithRetries` under the soft state - when the upgrade lands.
# `StorageURLSource::generate` must not rethrow that stale error over the real failure of the
# query: the peer whose error tore the pipeline down is the reason the query fails.
#
# The window between the throw and its handling is a few instructions wide, so the test holds the
# source in it with a failpoint: one source retries an always-failing URI and is parked right
# before its second attempt, the client is cancelled with SIGINT - `partial_result_on_first_cancel`
# makes that a soft cancellation - and releasing the parked source makes it throw the error of its
# interrupted read, which pauses on the second failpoint. Only then a second source, held by the
# test server until now, is released into a parse error: a real failure the cancellation has
# nothing to do with, which cancels the pipeline hard and upgrades the still-soft cancellation of
# the paused source. The released source must suppress its stale error.
#
# Parking the retrying source before the cancellation, instead of letting the cancellation find it
# in its backoff, is what makes the sequence deterministic: a source which is not parked can be
# anywhere - it can even run out of attempts and fail the query on its own - and the test would
# then wait for a pause which never comes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which counts the requests to each path, so that the test can sequence its steps on them.
# It binds to the port 0 and reports the port the kernel gave it, so that it cannot collide with
# anything else running in parallel.
#
# - /bad always answers 503: the source reading it spends its life in the retry backoff, which the
#   soft cancellation then interrupts.
# - /held blocks until /release is requested, then serves a row which does not parse as UInt64:
#   the hard failure of the peer, timed by the test.
# The server must serve requests in parallel: the test polls /stats and requests /release while
# the query's requests are being served, and a single-threaded server would block them.
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
            if not head:
                self.wfile.write(b'OK')
        elif self.path == '/held':
            release.wait()
            body = b'notanumber\n'
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
# The failpoint must not stay armed after the test: a query of a later test would pause on it with
# no one left to release it.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_retry_attempt" 2>/dev/null; $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_handling_interrupted_read_error" 2>/dev/null; kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

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

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_before_retry_attempt"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_url_pause_before_handling_interrupted_read_error"

# The waits for a failpoint to pause have no timeout of their own: bound them, so that a sequence
# which did not happen fails the test with a diagnostic instead of hanging it - and leaving the
# failpoints armed for the tests which run after it.
wait_for_pause()
{
    if ! timeout 300 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $1 PAUSE"; then
        echo "FAIL: the failpoint $1 was not reached"
        exit 1
    fi
}

QUERY_ID="${CLICKHOUSE_DATABASE}_hard_cancel_upgrade"

# The /bad source retries often enough to park on the failpoint quickly, and has enough attempts
# left not to run out of them before the test has delivered everything.
# max_threads 2 runs both sources concurrently.
# parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
# queries with their own query ids: the cancel of this client would not reach the sources the same
# way, and the logs the test waits for would be left under a different query id.
$CLICKHOUSE_CLIENT \
    --partial_result_on_first_cancel 1 \
    --http_make_head_request 0 \
    --http_max_tries 30 \
    --http_retry_initial_backoff_ms 1000 \
    --http_retry_max_backoff_ms 2000 \
    --max_threads 2 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "
        SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/bad', 'CSV', 'x UInt64')
        UNION ALL
        SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/held', 'CSV', 'x UInt64')" \
    >/dev/null 2>/dev/null &
CLIENT_PID=$!

# Wait until the query is registered - so that the SIGINT below cancels the query instead of a
# client that has not started it yet - and both sources have sent their requests: the /bad source
# has failed at least once and retries, the /held source is blocked in its read.
for _ in {1..300}; do
    [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done
for _ in {1..300}; do
    (($(stat_count "GET /bad") > 0)) && (($(stat_count "GET /held") > 0)) && break
    sleep 0.1
done

# The /bad source has failed its first attempt and is parked right before the second one, with the
# error of that attempt at hand: exactly the state in which a cancellation makes it throw.
wait_for_pause storage_url_pause_before_retry_attempt

# The soft cancellation: the client asks for its partial result, the query keeps running.
kill -SIGINT $CLIENT_PID

# Wait until the sources have seen it, so that the parked one is released into the cancelled state.
CANCELLED=0
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if (($($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message = 'The read has been cancelled, reason: PartialResult'") > 0)); then
        CANCELLED=1
        break
    fi
    sleep 0.1
done
if ((CANCELLED == 1)); then
    echo "the soft cancellation was delivered to the retrying source"
else
    echo "FAIL: the soft cancellation was not delivered to the retrying source"
fi

# Released into the cancelled state, the parked source throws the error of its interrupted read and
# pauses on the failpoint before that error is handled.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_retry_attempt"

wait_for_pause storage_url_pause_before_handling_interrupted_read_error
echo "the error of the interrupted read was caught before it was handled"

# With the thrown error held in the window, deliver the hard failure of the peer: the /held source
# reads a row which does not parse, and its error - one a cancellation has nothing to do with -
# cancels the pipeline hard, upgrading the paused source's soft cancellation.
curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

# Wait until the upgrade has been delivered to both sources, which leave a trace of each delivery.
UPGRADED=0
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    if (($($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message = 'The read has been cancelled, reason: Exception'") >= 2)); then
        UPGRADED=1
        break
    fi
    sleep 0.1
done
if ((UPGRADED == 1)); then
    echo "the hard cancellation upgrade was delivered while the error was held"
else
    echo "FAIL: the hard cancellation upgrade was not delivered while the error was held"
fi

# Only now release the held error into the upgraded state. It must be suppressed - the query fails
# with the error of the peer - not rethrown over it.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_url_pause_before_handling_interrupted_read_error"

wait $CLIENT_PID
CLIENT_STATUS=$?

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"

if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read was interrupted by a hard cancellation%'") != 0 ]]; then
    echo "the error of the interrupted read was suppressed after the upgrade"
else
    echo "FAIL: the error of the interrupted read was not suppressed after the upgrade"
fi

# The upgraded cancellation is not soft anymore: nothing may be discarded as a partial result.
if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read was interrupted by a cancellation after which the query returns its partial result%'") == 0 ]]; then
    echo "nothing was discarded as a partial result"
else
    echo "FAIL: the error was discarded as a partial result"
fi

# The query fails - the parse error of the peer is real - but not with the stale error of the
# interrupted read (code 86, RECEIVED_ERROR_FROM_REMOTE_IO_SERVER, would be the /bad source's 503).
# The code is read from the query log, not from the client's stderr: the server streams its logs to
# the client, and the parallel parsing of the /bad source logs the error of its interrupted read at
# the error level from its background thread, whichever error the query then fails with.
if ((CLIENT_STATUS != 0)); then
    echo "the query failed"
else
    echo "FAIL: the query succeeded"
fi
EXCEPTION_CODE=""
for _ in {1..300}; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    EXCEPTION_CODE=$($CLICKHOUSE_CLIENT --query "SELECT exception_code FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '$QUERY_ID' AND type = 'ExceptionWhileProcessing' LIMIT 1")
    [[ -n "$EXCEPTION_CODE" ]] && break
    sleep 0.1
done
if [[ -n "$EXCEPTION_CODE" && "$EXCEPTION_CODE" != 86 ]]; then
    echo "the query did not fail with the stale error of the interrupted read"
else
    echo "FAIL: the query failed with the stale error of the interrupted read (code '$EXCEPTION_CODE')"
fi
