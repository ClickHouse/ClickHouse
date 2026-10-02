#!/usr/bin/env bash
# Tags: no-fasttest
# Test that a soft cancellation - a `max_execution_time` timeout with the `break` overflow mode,
# after which the query must succeed with what it has already read - is honored the same way when
# the data is read by the background workers of a `ParallelReadBuffer`. `FormatFactory::getInput`
# wraps a large, seekable remote file in one, and a worker woken from `readBigAt` by the
# cancellation stores its `ReadInterruptedException` as the background exception of the buffer,
# which `ParallelReadBuffer::nextImpl` rethrows into `reader->pull`. The type of the exception
# survives that trip, so `StorageURLSource::generate` discards it as the error of the read it has
# cancelled itself - the query returns its partial result instead of failing with the interruption
# of a read no one was waiting for any more.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A server which reports a file large enough for `ParallelReadBuffer` to be used - the format
# factory requires at least two `max_download_buffer_size` segments - and serves the range requests
# of its workers. The first segment starts with a few good rows and then blocks, so that the
# workers are inside their reads when the cancellation arrives; the rest of the data is never sent.
# It binds to the port 0 and reports the port the kernel gave it, so that it cannot collide with
# anything else running in parallel, and serves requests in parallel: the test polls it while a
# range request is being held.
python3 -u -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

FILE_SIZE = 4 * 1024 * 1024

release = threading.Event()
ranges = 0
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def do_HEAD(self):
        if self.path == '/data':
            self.send_response(200)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Length', str(FILE_SIZE))
            self.send_header('Accept-Ranges', 'bytes')
            self.end_headers()
        else:
            self.send_response(200)
            self.send_header('Content-Length', '0')
            self.end_headers()

    def do_GET(self):
        global ranges
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
        elif self.path == '/ranges':
            with lock:
                body = str(ranges).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == '/data':
            header = self.headers.get('Range')
            if header is None:
                self.send_error(416)
                return
            begin, end = header.split('=')[1].split('-')
            begin, end = int(begin), int(end)
            with lock:
                ranges += 1
            self.send_response(206)
            self.send_header('Content-Type', 'text/csv')
            self.send_header('Content-Range', 'bytes {}-{}/{}'.format(begin, end, FILE_SIZE))
            self.send_header('Content-Length', str(end - begin + 1))
            self.end_headers()
            try:
                if begin == 0:
                    self.wfile.write(b'1\n2\n3\n')
                    self.wfile.flush()
                # The rest of the segment is never sent: the worker stays inside its read until the
                # cancellation wakes it up, and the connection is closed only afterwards.
                release.wait(60)
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

QUERY_ID="${CLICKHOUSE_DATABASE}_parallel_read_soft_cancel"
STDERR_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.stderr")

# `max_download_buffer_size` makes the reported file size two segments, which is the least the
# format factory accepts for the parallel read; `parallel_replicas_for_cluster_engines` would
# rewrite url to urlCluster and read it in remote queries with their own query ids, leaving the log
# the test waits for under a different query id.
$CLICKHOUSE_CLIENT \
    --max_execution_time 3 \
    --timeout_overflow_mode 'break' \
    --max_download_threads 2 \
    --max_download_buffer_size 2097152 \
    --parallel_replicas_for_cluster_engines 0 \
    --query_id "$QUERY_ID" \
    --query "SELECT x FROM url('http://127.0.0.1:$HTTP_PORT/data', 'CSV', 'x UInt64')" \
    >/dev/null 2>"$STDERR_FILE" &
CLIENT_PID=$!

# Wait until the workers of the parallel read are inside their range requests.
PARALLEL=0
for _ in {1..300}; do
    [[ $(curl -sS "http://127.0.0.1:$HTTP_PORT/ranges") -ge 2 ]] && PARALLEL=1 && break
    sleep 0.1
done

wait $CLIENT_PID
CLIENT_STATUS=$?

curl -sS "http://127.0.0.1:$HTTP_PORT/release" -o /dev/null

if ((PARALLEL == 1)); then
    echo "the data was read by the workers of the parallel read buffer"
else
    echo "FAIL: the data was not read by the workers of the parallel read buffer, $(curl -sS "http://127.0.0.1:$HTTP_PORT/ranges") range requests"
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
