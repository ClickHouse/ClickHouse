#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The data of an `INSERT` is parsed by the client, so it can fail in the middle of the exchange.
# The client recovers from it by sending `Cancel` and receiving the packets until the end of the
# stream, which leaves the protocol in sync. A session that continues after such a failure must not
# resynchronize the connection with a round trip: a `Pong` that does not arrive within
# `sync_request_timeout` is indistinguishable from a closed connection, and the client would
# silently reconnect - losing its temporary tables, current database and session settings.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything the
# server sends, while the client is configured with `sync_request_timeout` smaller than the delay.
# A client that pings after the failed insert cannot get the `Pong` in time, so it reconnects and
# loses the temporary table. A client that continues without the ping is unaffected - the delay only
# makes the queries themselves slower, which no timeout here objects to.

# The names are unique per run, so that concurrent runs of this test do not interfere.
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04811_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/04811_client_${CLICKHOUSE_DATABASE}.xml"

rm -f "$PROXY_PORT_FILE"

python3 "$CUR_DIR"/helpers/delaying_tcp_proxy.py "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT_TCP" 2 "$PROXY_PORT_FILE" &
PROXY_PID=$!
# shellcheck disable=SC2064
trap "kill $PROXY_PID 2>/dev/null; rm -f '$PROXY_PORT_FILE' '$CLIENT_CONFIG'" EXIT

for _ in {1..300}; do
    [ -s "$PROXY_PORT_FILE" ] && break
    sleep 0.1
done

PROXY_PORT=$(cat "$PROXY_PORT_FILE")

echo '<clickhouse><sync_request_timeout>1</sync_request_timeout></clickhouse>' > "$CLIENT_CONFIG"

# The address of the proxy replaces the address of the server, the rest of the options is kept.
CLIENT_OPT=$(echo "${CLICKHOUSE_CLIENT_OPT}" | sed "s/--host=[^ ]*//g; s/--port=[^ ]*//g")

# `async_insert` is disabled and `send_table_structure_on_insert_with_inline_data` is enabled
# explicitly (the test harness randomizes both): the data has to be parsed by the client for the
# failure to happen in the middle of the exchange. With either the asynchronous insert or the
# inline insert data mode the whole query including the data is parsed by the server, and the
# failure is an ordinary server exception - a different code path.
# The error of the second insert is expected; `UNKNOWN_TABLE` would mean that the client reconnected
# after it and lost the temporary table.
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --config-file "$CLIENT_CONFIG" --host 127.0.0.1 --port "$PROXY_PORT" --async_insert 0 --send_table_structure_on_insert_with_inline_data 1 --ignore-error --multiquery "
    CREATE TEMPORARY TABLE t_04811 (x UInt8);
    INSERT INTO t_04811 VALUES (1);
    INSERT INTO t_04811 VALUES (2), ('not a number');
    SELECT 'the session state is kept after a failure while sending the data' FROM t_04811 LIMIT 1;
" 2>&1 | grep -oF -e 'Cannot parse' -e 'UNKNOWN_TABLE' -e 'NETWORK_ERROR' -e 'ATTEMPT_TO_READ_AFTER_EOF' -e 'the session state is kept after a failure while sending the data'
