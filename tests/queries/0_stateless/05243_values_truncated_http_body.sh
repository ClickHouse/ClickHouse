#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `INSERT ... VALUES` over HTTP whose request body is shorter than its declared `Content-Length` must
# report that read failure and leave the server running, also when the row is cut in the middle of a value
# so that the format falls back to the expression parser while recovering from the parse error.
#
# The `X-ClickHouse-100-Continue: defer` header is load-bearing, not decoration: it is what makes the
# server keep the request body for the data format instead of reading it as query text, which is the
# documented way to send the query in the URL and the data in the body. Without it a short body is
# reported while still being read as query text and never reaches the format at all.
#
# curl cannot exercise this: it will not send a body shorter than the `Content-Length` it announced,
# so a raw socket is used, with a half-close so the response can still be read after the short body.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x UInt64) ENGINE = Memory"

truncated_post() {
    python3 -c "
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(30)
s.connect(('${CLICKHOUSE_HOST}', ${CLICKHOUSE_PORT_HTTP}))
s.sendall(b'''$1'''.replace(b'\n', b'\r\n'))
s.shutdown(socket.SHUT_WR)

data = b''
while True:
    try:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    except socket.timeout:
        data += b'TIMED-OUT-WAITING-FOR-RESPONSE'
        break
s.close()

print('read-failure-reported:', b'Code: 33' in data)
print('timed-out:', b'TIMED-OUT' in data)
"
}

echo "=== a row cut in the middle of a value reports the read failure ==="
truncated_post "POST /?database=${CLICKHOUSE_DATABASE}&query=INSERT+INTO+t+VALUES&async_insert=0 HTTP/1.1
Host: localhost
Expect: 100-continue
X-ClickHouse-100-Continue: defer
Content-Length: 1000

(12345"
echo "server-alive: $($CLICKHOUSE_CLIENT -q 'SELECT 1')"
$CLICKHOUSE_CLIENT -q "SELECT 'rows-visible', count() FROM t"

echo "=== control: a complete row followed by a truncated body reports the same read failure ==="
truncated_post "POST /?database=${CLICKHOUSE_DATABASE}&query=INSERT+INTO+t+VALUES&async_insert=0 HTTP/1.1
Host: localhost
Expect: 100-continue
X-ClickHouse-100-Continue: defer
Content-Length: 1000

(7)"
echo "server-alive: $($CLICKHOUSE_CLIENT -q 'SELECT 1')"
$CLICKHOUSE_CLIENT -q "SELECT 'rows-visible', count() FROM t"

echo "=== control: a value the streaming parser cannot read is still evaluated by the expression parser ==="
$CLICKHOUSE_CLIENT -q "INSERT INTO t VALUES (1+1)"
$CLICKHOUSE_CLIENT -q "SELECT x FROM t ORDER BY x"
