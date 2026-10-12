#!/usr/bin/env bash

CLICKHOUSE_PORT_TCP=50111
CLICKHOUSE_DATABASE=default

# This test starts a second server with the *embedded* default config, which opens the HTTP, MySQL,
# PostgreSQL and interserver listeners besides the TCP one. Overriding only `tcp_port` leaves those
# at their defaults, so the throwaway server competes for ports the main test server serves. It can
# win: on macOS the main server binds only `::`, which leaves `127.0.0.1` free for a second bind of
# the same port. `clickhouse-test` creates and drops every per-test database over HTTP on 8123, so
# those requests would land on this server - which has its own empty `--path` - and unrelated tests
# would then fail with `UNKNOWN_DATABASE`. Give every listener a port of its own.
SERVER_PORT_HTTP=50112
SERVER_PORT_MYSQL=50113
SERVER_PORT_POSTGRESQL=50114
SERVER_PORT_INTERSERVER=50115

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting clickhouse-server"

$CLICKHOUSE_BINARY server -- --tcp_port "$CLICKHOUSE_PORT_TCP" --http_port "$SERVER_PORT_HTTP" \
    --mysql_port "$SERVER_PORT_MYSQL" --postgresql_port "$SERVER_PORT_POSTGRESQL" \
    --interserver_http_port "$SERVER_PORT_INTERSERVER" \
    --path "${CLICKHOUSE_TMP}/" > "${CLICKHOUSE_TMP}/server.log" 2>&1 &
PID=$!

function finish()
{
    kill $PID
    wait
}
trap finish EXIT

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" 2>/dev/null && break
    if [[ $i == 30 ]]; then
        cat "${CLICKHOUSE_TMP}/server.log"
        exit 1
    fi
done

# Check access rights

$CLICKHOUSE_CLIENT --query "
    DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1};
    CREATE DATABASE ${CLICKHOUSE_DATABASE_1};
    USE ${CLICKHOUSE_DATABASE_1};

    CREATE TABLE t (s String) ENGINE=TinyLog;
    INSERT INTO t VALUES ('Hello');
    SELECT * FROM t;
    DROP TABLE t;

    CREATE TEMPORARY TABLE t (s String);
    INSERT INTO t VALUES ('World');
    SELECT * FROM t;
"

kill $PID
# Dump server.log in case wait hangs
function trace()
{
    # clickhouse-test prints only stderr on timeouts
    cat "${CLICKHOUSE_TMP}/server.log" >&2
}
trap trace EXIT
wait
trap '' EXIT
