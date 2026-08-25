#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The HTTP interface of clickhouse-local must answer CORS preflight requests with the same
# permissive headers as the default clickhouse-server configuration, so that a browser can talk
# to it out of the box - including the web UI opened from a `file://` URL, whose origin is `null`.
# clickhouse-local usually runs without a config file, so these defaults cannot come from one.

PORT_FILE="${CLICKHOUSE_TMP}/$(basename "${BASH_SOURCE[0]}" .sh).port"
rm -f "$PORT_FILE"

# Bind an HTTP listener on an OS-assigned port (`--http_port 0`) to stay parallel-safe, publish
# the bound port via INTO OUTFILE, then keep the process alive long enough to probe it.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --http_port 0 \
    --tcp_port 0 \
    --query "
    SYSTEM START LISTEN HTTP;
    SELECT getServerPort('http_port') INTO OUTFILE '${PORT_FILE}' FORMAT TSVRaw;
    SELECT sleep(3) FROM numbers(20) SETTINGS max_block_size = 1 FORMAT Null;
    " >/dev/null 2>&1 &
LOCAL_PID=$!

PORT=""
for _ in $(seq 1 100); do
    if [ -s "$PORT_FILE" ]; then PORT=$(cat "$PORT_FILE"); break; fi
    sleep 0.1
done

if [ -z "$PORT" ]; then
    echo "FAIL: HTTP listener did not start"
else
    # The preflight of a request carrying an `Authorization` header, as sent by the web UI.
    echo -n 'preflight: '
    curl -s -D - -o /dev/null --max-time 30 -X OPTIONS \
        -H 'Origin: null' \
        -H 'Access-Control-Request-Method: POST' \
        -H 'Access-Control-Request-Headers: authorization' \
        "http://127.0.0.1:${PORT}/?query" | grep -ci '^access-control-allow-origin: \*'

    echo -n 'preflight authorization: '
    curl -s -D - -o /dev/null --max-time 30 -X OPTIONS \
        -H 'Origin: null' \
        -H 'Access-Control-Request-Method: POST' \
        -H 'Access-Control-Request-Headers: authorization' \
        "http://127.0.0.1:${PORT}/?query" | grep -Eic '^access-control-allow-headers:.*authorization'

    echo -n 'preflight method: '
    curl -s -D - -o /dev/null --max-time 30 -X OPTIONS \
        -H 'Origin: null' \
        -H 'Access-Control-Request-Method: POST' \
        -H 'Access-Control-Request-Headers: authorization' \
        "http://127.0.0.1:${PORT}/?query" | grep -Eic '^access-control-allow-methods:.*POST'

    # The response to the request itself must be readable by the page as well.
    echo -n 'query: '
    curl -s -D - -o /dev/null --max-time 30 -H 'Origin: null' --data-binary 'SELECT 1' \
        "http://127.0.0.1:${PORT}/" | grep -ci '^access-control-allow-origin: \*'

    # Without an `Origin` there is no cross-origin request and no CORS headers.
    echo -n 'no origin: '
    curl -s -D - -o /dev/null --max-time 30 --data-binary 'SELECT 1' \
        "http://127.0.0.1:${PORT}/" | grep -ci '^access-control-allow-origin:'
fi

kill "$LOCAL_PID" 2>/dev/null
wait "$LOCAL_PID" 2>/dev/null
rm -f "$PORT_FILE"

# Headers passed through the post-`--` configuration surface must override the defaults.
$CLICKHOUSE_LOCAL \
    --listen_host 127.0.0.1 \
    --http_port 0 \
    --tcp_port 0 \
    --query "
    SYSTEM START LISTEN HTTP;
    SELECT getServerPort('http_port') INTO OUTFILE '${PORT_FILE}' FORMAT TSVRaw;
    SELECT sleep(3) FROM numbers(20) SETTINGS max_block_size = 1 FORMAT Null;
    " \
    -- \
    --http_options_response.header[0].name=Access-Control-Allow-Origin \
    --http_options_response.header[0].value=https://example.com >/dev/null 2>&1 &
LOCAL_PID=$!

PORT=""
for _ in $(seq 1 100); do
    if [ -s "$PORT_FILE" ]; then PORT=$(cat "$PORT_FILE"); break; fi
    sleep 0.1
done

if [ -z "$PORT" ]; then
    echo "FAIL: HTTP listener with custom CORS configuration did not start"
else
    echo -n 'custom origin: '
    curl -s -D - -o /dev/null --max-time 30 -X OPTIONS \
        -H 'Origin: null' \
        -H 'Access-Control-Request-Method: POST' \
        "http://127.0.0.1:${PORT}/?query" | grep -ci '^access-control-allow-origin: https://example.com'
fi

kill "$LOCAL_PID" 2>/dev/null
wait "$LOCAL_PID" 2>/dev/null
rm -f "$PORT_FILE"
