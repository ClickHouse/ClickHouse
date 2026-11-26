#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A SQL-defined handler that never consumes the request body accepts a lengthless non-chunked POST/PUT
# (see 04821_handler_bodyless_post). Such a request exposes its body as an EOF-delimited stream that the
# server never reads, so connection reuse would misinterpret any bytes the client did send as the beginning
# of the next request. This test pins the framing safety of that acceptance: the server must close the
# connection (`Connection: close` + actual socket close) instead of keeping it alive, so a pipelined
# "poisoning" request smuggled as the unread body is never parsed or executed.
#
# curl cannot exercise this: it never sends a lengthless body followed by another request on the same
# connection, so a raw socket is used. The HTTP port is used directly (plain HTTP).

DB="${CLICKHOUSE_DATABASE}"
P="/hka_${DB}"
H="hka_${DB}"

cleanup() {
    $CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`;"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -q "CREATE HANDLER \`$H\` URL '${P}' METHODS (POST, PUT) AS SELECT 1 AS a FORMAT TSV"

raw_http() {
    python3 -c "
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(30)
s.connect(('${CLICKHOUSE_HOST}', ${CLICKHOUSE_PORT_HTTP}))
s.sendall(b'''$1'''.replace(b'\n', b'\r\n'))

data = b''
while True:
    try:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    except socket.timeout:
        data += b'TIMED-OUT-WAITING-FOR-SERVER-CLOSE'
        break
s.close()

print('responses:', data.count(b'HTTP/1.1 '))
print('status-200:', data.count(b'HTTP/1.1 200'))
print('connection-close:', data.lower().count(b'connection: close'))
print('smuggled-query-executed:', b'42424242' in data)
print('timed-out:', b'TIMED-OUT' in data)
"
}

echo "=== a lengthless POST with a pipelined request smuggled as the unread body gets one response and a closed connection ==="
raw_http "POST ${P} HTTP/1.1
Host: localhost
Connection: keep-alive

GET /?query=SELECT+42424242 HTTP/1.1
Host: localhost

"

echo "=== same for PUT ==="
raw_http "PUT ${P} HTTP/1.1
Host: localhost
Connection: keep-alive

GET /?query=SELECT+42424242 HTTP/1.1
Host: localhost

"

echo "=== control: with Content-Length the connection stays reusable for a second request ==="
# Sequential reuse (send - read response - send again on the same socket): the server does not support
# pipelining, so the second request is written only after the first chunked response is fully received.
python3 -c "
import socket

def read_response(s):
    data = b''
    while b'0\r\n\r\n' not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    return data

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(30)
s.connect(('${CLICKHOUSE_HOST}', ${CLICKHOUSE_PORT_HTTP}))

s.sendall(b'POST ${P} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n')
first = read_response(s)
s.sendall(b'POST ${P} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
second = read_response(s)
s.close()

data = first + second
print('responses:', data.count(b'HTTP/1.1 '))
print('status-200:', data.count(b'HTTP/1.1 200'))
print('first-response-closed:', b'onnection: close' in first.lower())
"
