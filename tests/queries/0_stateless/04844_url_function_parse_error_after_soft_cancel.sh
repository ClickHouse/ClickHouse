#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a soft cancellation - a `max_execution_time` timeout with the `break` overflow mode,
# after which the query must succeed with what it has already read - does not turn an unrelated
# error into a successful partial result. `StorageURLSource` discards the error of the read it has
# cancelled itself, for example the last HTTP error rethrown when the cancellation wakes up the
# retry backoff, see `ReadWriteBufferFromHTTP::doWithRetries` - but a parse error of the data is
# not that error: the file is malformed no matter when the query stopped wanting more of it, and
# the query must fail with it the same way it would with no cancellation at all, see
# `StorageURLSource::generate`. The server streams a few good rows, withholds the rest until the
# cancellation has been delivered, and only then sends a malformed row.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server whose /stream response stops in the middle of the data, so that the cancellation can be
# delivered while the source is blocked reading it, and resumes with a malformed row when the test
# requests /release. It binds to the port 0 and reports the port the kernel gave it, so that it
# cannot collide with anything else running in parallel. The server must serve requests in
# parallel: the test requests /release and /reset while /stream is being served, and a
# single-threaded server would block them until the query has already finished, too late to cancel
# it.
python3 -u -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

release = threading.Event()
streams = 0

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        global streams
        if self.path == '/health':
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b'OK')
        elif self.path == '/release':
            release.set()
            self.send_response(200)
            self.end_headers()
        elif self.path == '/reset':
            release.clear()
            self.send_response(200)
            self.end_headers()
        elif self.path == '/streams':
            body = str(streams).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/stream':
            streams += 1
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.end_headers()
            try:
                self.wfile.write(b'1\n2\n3\n')
                release.wait(60)
                self.wfile.write(b'not a number\n')
            except BrokenPipeError:
                pass  # The reader may be gone by the time the response is released.
        else:
            self.send_error(503)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

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

# The cancellation must land while the source is blocked in the middle of the data, before the
# malformed row. When the server is slow to start the query - under a sanitizer, for example - the
# soft timeout can fire before the data request has been made, and the query ends without ever
# seeing the malformed row: nothing to assert then, so retry.
for attempt in {1..10}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/reset" -o /dev/null

    QUERY_ID="${CLICKHOUSE_DATABASE}_parse_error_after_soft_cancel_$attempt"
    STDERR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.stderr")

    # parallel_replicas_for_cluster_engines would rewrite url to urlCluster and read it in remote
    # queries with their own query ids: the log the test waits for would be left under a different
    # query id.
    $CLICKHOUSE_CLIENT \
        --max_execution_time 1 \
        --timeout_overflow_mode 'break' \
        --parallel_replicas_for_cluster_engines 0 \
        --query_id "$QUERY_ID" \
        --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/stream', 'CSV', 'x UInt64')" \
        >/dev/null 2>"$STDERR_FILE" &
    CLIENT_PID=$!

    # Wait until the source is inside the request for the data.
    for _ in {1..100}; do
        [[ $(curl -sS "http://127.0.0.1:$HTTP_PORT/streams") != 0 ]] && break
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

    # Only now, with the cancellation delivered, let the server send the malformed row the source
    # is blocked waiting for.
    curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

    wait $CLIENT_PID
    CLIENT_STATUS=$?

    if ((DELIVERED == 1)) && [[ $(curl -sS "http://127.0.0.1:$HTTP_PORT/streams") != 0 ]]; then
        break
    fi
    rm -f "$STDERR_FILE"
done

if ((DELIVERED == 1)); then
    echo "the cancellation was delivered while the source was blocked in the data"
else
    echo "FAIL: the cancellation was not delivered while the source was blocked in the data"
fi

# The malformed row must fail the query even though it arrived after the soft cancellation: it is
# not the error of the read the source has cancelled itself. The wording of the parse error differs
# between the parallel and the single-threaded parsing, so accept both.
if ((CLIENT_STATUS != 0)) && grep -q -i -E 'cannot parse|is not like' "$STDERR_FILE"; then
    echo "the query failed with the parse error"
else
    echo "FAIL: the query did not fail with the parse error, status $CLIENT_STATUS, stderr:"
    cat "$STDERR_FILE"
fi

rm -f "$STDERR_FILE"
