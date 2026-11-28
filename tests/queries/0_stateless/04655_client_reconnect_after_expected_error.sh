#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some queries desynchronize the protocol: the client sends the query followed by the (empty) block
# of external data, the server throws before it reads that block, and then it closes the connection
# with `Unexpected packet Data received from client`. This happens for every `serverError` hint in a
# `.sql` test, so the client recovers by reconnecting after an expected error.
#
# The recovery must not depend on the close having already arrived - a non-blocking check of the
# socket cannot see a connection that the server has just closed. The client has to ask the server
# and wait for the answer, so that everything the server has already sent is observed.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything the
# server sends, the close included. A client that only looks at the socket does not notice that the
# connection is gone and fails the next query with `ATTEMPT_TO_READ_AFTER_EOF`.

# The name is unique per run, so that concurrent runs of this test do not interfere.
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04655_proxy_${CLICKHOUSE_DATABASE}.port"

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

# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --host 127.0.0.1 --port "$PROXY_PORT" --multiquery "
    CREATE DATABASE ${CLICKHOUSE_DATABASE}_04655 SETTINGS x = 1; -- { serverError UNKNOWN_SETTING }
    SELECT 'The connection is usable after an expected error';
"
