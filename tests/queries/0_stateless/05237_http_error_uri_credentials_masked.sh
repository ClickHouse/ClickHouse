#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each check looks only at the URI token of the message, i.e. up to the first space after
# "Received error from remote server". The response body that follows it, and the trailing
# "(in file/uri ...)" suffix that comes from getFileName(), are masked elsewhere and are not
# what this test pins.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# Answers every request with 403 after consuming the request body, so an INSERT completes
# instead of blocking on an unread chunked body.
python3 -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def drain(self):
        if 'chunked' in (self.headers.get('Transfer-Encoding') or '').lower():
            while True:
                line = self.rfile.readline()
                if not line:
                    return
                size = int(line.split(b';')[0].strip() or b'0', 16)
                if size == 0:
                    self.rfile.readline()
                    return
                self.rfile.read(size)
                self.rfile.readline()
        else:
            length = int(self.headers.get('Content-Length') or 0)
            if length:
                self.rfile.read(length)

    def respond(self, with_body=True):
        self.drain()
        status, body = (200, b'ready') if self.path == '/ready' else (403, b'denied')
        self.send_response(status)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def do_GET(self):
        self.respond()

    def do_PUT(self):
        self.respond()

    def do_POST(self):
        self.respond()

    def do_HEAD(self):
        self.respond(with_body=False)

    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

trap 'kill $HTTP_PID 2>/dev/null' EXIT

for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/ready")" = "200" ]; then
        break
    fi
    sleep 0.1
done

# Reports every secret as absent and every replacement and control as present, so a message
# that never carried the credential in the first place cannot be read as a pass. An argument
# prefixed with '-' must NOT appear, any other argument must.
check_uri()
{
    local uri_token
    uri_token=$(grep -oE 'Received error from remote server [^ ]+' | head -n 1)

    if [ -z "$uri_token" ]; then
        echo "no message"
        return
    fi

    local expectation
    for expectation in "$@"; do
        case "$expectation" in
            -*)
                if [[ "$uri_token" == *"${expectation#-}"* ]]; then
                    echo "LEAKED ${expectation#-}"
                else
                    echo "hidden ${expectation#-}"
                fi
                ;;
            *)
                if [[ "$uri_token" == *"$expectation"* ]]; then
                    echo "present $expectation"
                else
                    echo "MISSING $expectation"
                fi
                ;;
        esac
    done
}

echo "--- query parameters, INSERT (assertResponseIsOk) ---"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO TABLE FUNCTION
        url('http://127.0.0.1:${HTTP_PORT}/upload?X-Amz-Signature=siguri9f2a&password=pwuri3k8&list-type=2', 'CSV', 'c0 UInt8')
    SELECT 1
" 2>&1 | check_uri -siguri9f2a -pwuri3k8 'X-Amz-Signature=[HIDDEN]' 'password=[HIDDEN]' 'list-type=2'

echo "--- userinfo, SELECT ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT * FROM url('http://leakuser:pwuri5x9@127.0.0.1:${HTTP_PORT}/download', 'CSV', 'id UInt64')
" 2>&1 | check_uri -pwuri5x9 '[HIDDEN]@' '127.0.0.1'
