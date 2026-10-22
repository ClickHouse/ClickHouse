#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local must answer an HTTP OPTIONS request (the CORS preflight) with a proper
# response even when there is no `http_options_response` section in the config (which is the
# usual case for clickhouse-local, since it runs without a config file). Otherwise the
# connection is closed without any HTTP response and the client sees an empty reply: the web
# UI (`/play`) connection health-check is an OPTIONS request, so it would report the
# connection as broken even though ordinary queries work.

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
    # The OPTIONS preflight must get an HTTP response. Before the fix clickhouse-local closed the
    # connection without any response, which curl reports as status 000 (empty reply).
    curl -s -o /dev/null -w '%{http_code}\n' --max-time 30 -X OPTIONS "http://127.0.0.1:${PORT}/?query"
fi

kill "$LOCAL_PID" 2>/dev/null
wait "$LOCAL_PID" 2>/dev/null
rm -f "$PORT_FILE"
