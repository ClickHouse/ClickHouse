#!/usr/bin/env bash
# Tags: no-random-settings, no-fasttest, long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export MY_CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --async_insert_busy_timeout_ms 10 --async_insert_max_data_size 1 --async_insert 1"

function insert1()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${MY_CLICKHOUSE_CLIENT} --wait_for_async_insert 0 -q 'INSERT INTO async_inserts_race FORMAT CSV 1,"a"'
    done
}

function insert2()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${MY_CLICKHOUSE_CLIENT} --wait_for_async_insert 0 -q 'INSERT INTO async_inserts_race FORMAT JSONEachRow {"id": 5, "s": "e"} {"id": 6, "s": "f"}'
    done
}

function insert3()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${MY_CLICKHOUSE_CLIENT} --wait_for_async_insert 1 -q "INSERT INTO async_inserts_race VALUES (7, 'g') (8, 'h')" &
        sleep 0.05
    done

    wait
}

function select1()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${MY_CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts_race FORMAT Null"
    done
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts_race"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts_race (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

export -f insert1
export -f insert2
export -f insert3
export -f select1

TIMEOUT=10

for _ in {1..3}; do
    insert1 $TIMEOUT &
    insert2 $TIMEOUT &
    insert3 $TIMEOUT &
done

select1 $TIMEOUT &

wait
echo "OK"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts_race";
