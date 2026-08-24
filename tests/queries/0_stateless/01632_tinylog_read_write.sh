#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test; CREATE TABLE IF NOT EXISTS test (x UInt64, s Array(Nullable(String))) ENGINE = TinyLog;"

function thread_select {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --local_filesystem_read_method pread --query "SELECT * FROM test FORMAT Null"
        sleep 0.0$RANDOM
    done
}

function thread_insert {
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$1" ]; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO test VALUES (1, ['Hello'])"
        sleep 0.0$RANDOM
    done
}

export -f thread_select
export -f thread_insert


# Do randomized queries and expect nothing extraordinary happens.

TIMEOUT=10

thread_select $TIMEOUT &
thread_select $TIMEOUT &
thread_select $TIMEOUT &
thread_select $TIMEOUT &

thread_insert $TIMEOUT &
thread_insert $TIMEOUT &
thread_insert $TIMEOUT &
thread_insert $TIMEOUT &

wait
echo "Done"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test;"
