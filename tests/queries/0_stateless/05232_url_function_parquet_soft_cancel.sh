#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a soft cancellation - a `max_execution_time` timeout with the `break` overflow mode,
# after which the query must succeed with what it has already read - is honored the same way when
# the data is read by the background tasks of the native `Parquet` reader. Those tasks read the
# column chunks with positional reads of the seekable HTTP buffer, and hand their failures over to
# the reading thread through an `std::exception_ptr` which `Parquet::Prefetcher::rethrowException`
# and `Parquet::ReadManager::read` pass through `copyMutableException`. That copy rethrows the
# exception through the virtual `Poco::Exception::rethrow`, so an exception type which does not
# override `rethrow` is sliced back to a plain `Exception` on the way. `ReadInterruptedException`
# does override it, so `StorageURLSource::generate` still recognizes the error of the read it has
# cancelled itself and discards it - the query returns its partial result instead of failing with
# the interruption of a read no one was waiting for any more.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.parquet")
PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# Large enough for the column chunks to be read separately from the metadata at the end of the file.
$CLICKHOUSE_LOCAL --query "SELECT number AS x, toString(number) AS s FROM numbers(3000000) FORMAT Parquet" > "$DATA_FILE"

# A server which reports the file as seekable and serves the positional reads of the reader. The
# reads of the footer and of the metadata land in the last bytes of the file and are answered in
# full; every read of the data itself - anything that begins before that tail - is accepted and
# then left hanging, so that a background task of the reader is inside a read of a column chunk
# when the cancellation arrives. Keying the decision on the offset rather than on the number of
# requests already served is what makes it deterministic: how many requests the metadata takes,
# and how the reader splits the data reads, both depend on the build and on the settings. It binds
# to the port 0 and reports the port the kernel gave it, so that it cannot collide with anything
# else running in parallel, and serves requests in parallel: the test asks it for the number of
# reads it has held while the query is running.
python3 -u -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

with open('$DATA_FILE', 'rb') as f:
    DATA = f.read()
FILE_SIZE = len(DATA)

# The footer and the metadata of a Parquet file live in its last bytes and are read from there.
METADATA_TAIL = 1024 * 1024

release = threading.Event()
requests = 0
blocked = 0
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_HEAD(self):
        if self.path == '/data':
            self.send_response(200)
            self.send_header('Content-Type', 'application/octet-stream')
            self.send_header('Content-Length', str(FILE_SIZE))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header('Content-Length', '0')
            self.end_headers()

    def do_GET(self):
        global requests, blocked
        if self.path == '/health':
            body = b'OK'
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/release':
            release.set()
            self.send_response(200)
            self.send_header('Content-Length', '0')
            self.end_headers()
        elif self.path == '/blocked':
            with lock:
                body = str(blocked).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/data':
            header = self.headers.get('Range')
            if header is None:
                begin, end = 0, FILE_SIZE - 1
            else:
                begin, end = header.split('=')[1].split('-')
                begin, end = int(begin), min(int(end), FILE_SIZE - 1)
            hold = begin < FILE_SIZE - METADATA_TAIL
            with lock:
                requests += 1
                if hold:
                    blocked += 1
            self.send_response(206 if header is not None else 200)
            self.send_header('Content-Type', 'application/octet-stream')
            if header is not None:
                self.send_header('Content-Range', 'bytes {}-{}/{}'.format(begin, end, FILE_SIZE))
            self.send_header('Content-Length', str(end - begin + 1))
            self.end_headers()
            try:
                if hold:
                    # Nothing is sent: the reader stays inside its read until it gives up on it, and
                    # the connection is closed only after the query has finished.
                    release.wait(120)
                else:
                    self.wfile.write(DATA[begin:end + 1])
                    self.wfile.flush()
            except BrokenPipeError:
                pass  # The reader may be gone by the time the response is released.
            self.close_connection = True
        else:
            self.send_error(503)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$DATA_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")

for _ in {1..300}; do
    curl -sS "http://127.0.0.1:$HTTP_PORT/health" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

QUERY_ID="${CLICKHOUSE_DATABASE}_parquet_soft_cancel"
STDERR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.stderr")

# The cancellation must arrive while a read of a column chunk is in flight. The execution time
# limit is therefore above the time the metadata phase needs even on a loaded sanitizer runner, and
# the budget of the retries of the held read - `http_max_tries` attempts of `http_receive_timeout`
# each - is far above the limit, so that the read is still being retried when the limit fires.
# `parallel_replicas_for_cluster_engines` would rewrite url to urlCluster and read it in remote
# queries with their own query ids, leaving the log the test looks for under a different query id.
$CLICKHOUSE_CLIENT \
    --max_execution_time 10 \
    --timeout_overflow_mode 'break' \
    --http_receive_timeout 6 \
    --http_max_tries 20 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT sum(x) FROM url('http://127.0.0.1:$HTTP_PORT/data', 'Parquet')" \
    >/dev/null 2>"$STDERR_FILE"
CLIENT_STATUS=$?

# The counter is cumulative, so it is read after the query: a read of the data which was held is
# still counted when the query has already left it, and the test does not have to poll for it.
HELD=$(curl -sS "http://127.0.0.1:$HTTP_PORT/blocked" || echo 0)

curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

if ((${HELD:-0} >= 1)); then
    echo "a read of the parquet data was in flight"
else
    echo "FAIL: no read of the parquet data was in flight"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
if [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.text_log WHERE query_id = '$QUERY_ID' AND logger_name = 'StorageURLSource' AND message LIKE 'The read was interrupted by a cancellation after which the query returns its partial result%'") != 0 ]]; then
    echo "the interrupted background read was discarded"
else
    echo "FAIL: the interrupted background read was not discarded"
fi

if ((CLIENT_STATUS == 0)); then
    echo "the query returned its partial result"
else
    echo "FAIL: the query failed with status $CLIENT_STATUS, stderr:"
    cat "$STDERR_FILE"
fi

rm -f "$STDERR_FILE"
