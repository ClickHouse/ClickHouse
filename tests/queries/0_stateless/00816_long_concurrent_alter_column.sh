#!/usr/bin/env bash
# Tags: long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS concurrent_alter_column;
    CREATE TABLE concurrent_alter_column (ts DATETIME) ENGINE = MergeTree PARTITION BY toStartOfDay(ts) ORDER BY tuple();
"
function thread1()
{
    for i in {1..500}; do
        echo "ALTER TABLE concurrent_alter_column ADD COLUMN c$i DOUBLE;"
    done | ${CLICKHOUSE_CLIENT} -n
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column ADD COLUMN d DOUBLE"
    sleep 0.0$RANDOM
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column DROP COLUMN d"
}

function thread3()
{
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column ADD COLUMN e DOUBLE"
    sleep 0.0$RANDOM
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column DROP COLUMN e"
}

function thread4()
{
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column ADD COLUMN f DOUBLE"
    sleep 0.0$RANDOM
    $CLICKHOUSE_CLIENT --query "ALTER TABLE concurrent_alter_column DROP COLUMN f"
}

export -f thread1
export -f thread2
export -f thread3
export -f thread4

TIMEOUT=30

clickhouse_client_loop_timeout $TIMEOUT thread1 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread2 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread3 2> /dev/null &
clickhouse_client_loop_timeout $TIMEOUT thread4 2> /dev/null &

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE concurrent_alter_column NO DELAY"
echo 'did not crash'
