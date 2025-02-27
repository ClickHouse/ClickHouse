#!/usr/bin/env bash
# Tags: no-parallel

CLICKHOUSE_PORT_TCP=50111
CLICKHOUSE_DATABASE=default

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Starting clickhouse-server"

$PORT

$CLICKHOUSE_BINARY server -- --tcp_port "$CLICKHOUSE_PORT_TCP" --path /tmp/ > server.log 2>&1 &
PID=$!

function finish {
    kill $PID
    wait
}
trap finish EXIT

echo "Waiting for clickhouse-server to start"

for i in {1..30}; do
    sleep 1
    $CLICKHOUSE_CLIENT --query "SELECT 1" 2>/dev/null && break
    if [[ $i == 30 ]]; then
        cat server.log
        exit 1
    fi
done

# Check access rights

$CLICKHOUSE_CLIENT -n --query "
    DROP DATABASE IF EXISTS test;
    CREATE DATABASE test;
    USE test;

    CREATE TABLE t (s String) ENGINE=TinyLog;
    INSERT INTO t VALUES ('Hello');
    SELECT * FROM t;
    DROP TABLE t;

    CREATE TEMPORARY TABLE t (s String);
    INSERT INTO t VALUES ('World');
    SELECT * FROM t;
";
