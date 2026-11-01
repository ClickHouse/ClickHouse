#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query can fail inside `Connection::sendQuery` before the first byte of the query packet has
# been written - for example, on the validation of `network_compression_method`. Such a failure
# leaves the protocol in sync, so a session that continues after it must not resynchronize the
# connection with a round trip: a `Pong` that does not arrive within `sync_request_timeout` is
# indistinguishable from a closed connection, and the client would silently reconnect - losing its
# temporary tables, current database and session settings.
#
# Unlike a failure on the local checks of the client (`INTO OUTFILE` and such - see
# 04695_client_no_reconnect_after_local_error), this one happens after the resynchronization flag
# has been armed for the query exchange, so it exercises the disarming on a preflight failure.
#
# The check below is deterministic: all the traffic goes through a proxy that delays everything the
# server sends, while the client is configured with `sync_request_timeout` smaller than the delay.
# A client that pings after the preflight error cannot get the `Pong` in time, so it reconnects and
# loses the temporary table. A client that continues without the ping is unaffected - the delay only
# makes the queries themselves slower, which no timeout here objects to.

# The names are unique per run, so that concurrent runs of this test do not interfere.
PROXY_PORT_FILE="${CLICKHOUSE_TMP}/04818_proxy_${CLICKHOUSE_DATABASE}.port"
CLIENT_CONFIG="${CLICKHOUSE_TMP}/04818_client_${CLICKHOUSE_DATABASE}.xml"

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

# `T64` is rejected by the preflight of `Connection::sendQuery` before anything has been sent
# (per-query `SETTINGS` are applied to the client context temporarily, so only this query fails).
# `BAD_ARGUMENTS` is expected; `UNKNOWN_TABLE` would mean that the client reconnected after the
# preflight error and lost the temporary table.
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT_BINARY} ${CLIENT_OPT} --config-file "$CLIENT_CONFIG" --host 127.0.0.1 --port "$PROXY_PORT" --ignore-error --multiquery "
    CREATE TEMPORARY TABLE t_04818 (x UInt8);
    INSERT INTO t_04818 VALUES (1);
    SELECT x FROM t_04818 SETTINGS network_compression_method = 'T64';
    SELECT 'the session state is kept after a preflight error', x FROM t_04818;
" 2>&1 | grep -oF -e 'BAD_ARGUMENTS' -e 'UNKNOWN_TABLE' -e 'NETWORK_ERROR' -e 'the session state is kept after a preflight error'
