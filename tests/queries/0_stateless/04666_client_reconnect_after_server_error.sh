#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failed query can leave the protocol desynchronized even when the error is not expected by a test
# hint: the server throws before it reads the block of external data of that query, and then closes
# the connection. Whenever the session continues after such an error - in the interactive mode, or
# with `--ignore-error` - the client has to resynchronize the connection with a round trip, because
# a check of the socket alone does not see a close that has not been delivered yet.
#
# All the traffic goes through a proxy that delays everything the server sends, the close included,
# which makes the race deterministic: without the resynchronization the query that follows the error
# is sent into the connection that the server has already closed and reports
# `ATTEMPT_TO_READ_AFTER_EOF` (it is retried on a new connection afterwards, so the error itself is
# what the test looks at).

# The name is unique per run, so that concurrent runs of this test do not interfere.
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04666_proxy_${CLICKHOUSE_DATABASE}.port"

rm -f "$PROXY_PORT_FILE"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 1 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_PORT_FILE'" EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

# The address of the proxy replaces the address of the server, the rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# The error is not annotated with a test hint, so the recovery cannot rely on the expected-error path.
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --host 127.0.0.1 --port "$PROXY_PORT" --ignore-error --multiquery "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_04666 SETTINGS x = 1;
    SELECT 'The connection is usable after a server error';
" 2>&1 | grep -oF -e 'UNKNOWN_SETTING' -e 'ATTEMPT_TO_READ_AFTER_EOF' -e 'NETWORK_ERROR' -e 'The connection is usable after a server error'
