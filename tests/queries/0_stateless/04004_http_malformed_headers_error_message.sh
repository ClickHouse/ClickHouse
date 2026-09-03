#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that malformed HTTP headers return 400 with an error message in the body,
# not just an empty response. https://github.com/ClickHouse/ClickHouse/issues/98250
#
# We send a raw HTTP request with a bare \n (instead of \r\n) inside a header value.
# curl cannot be used here because it normalizes headers.

python3 -c "
import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(('${CLICKHOUSE_HOST}', ${CLICKHOUSE_PORT_HTTP}))
# Note: the header 'X-Test: bad\nY: value' contains a bare LF, which is invalid HTTP.
s.sendall(b'GET /?query=SELECT+1 HTTP/1.1\r\nHost: localhost\r\nX-Test: bad\nY: value\r\n\r\n')

data = b''
while True:
    try:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    except socket.timeout:
        break
s.close()

# The response body (after the empty line) should contain the error message.
body = data.split(b'\r\n\r\n', 1)[1] if b'\r\n\r\n' in data else b''
print(body.decode())
"
