#!/usr/bin/env bash
# Tags: long, no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function insert1()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"

    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
1,"a"
2,"b"
'
    done
}

function insert2()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"

    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 5, "s": "e"} {"id": 6, "s": "f"}'
    done
}

function insert3()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"

    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO FUNCTION remote('127.0.0.1', $CLICKHOUSE_DATABASE, async_inserts) VALUES (7, 'g') (8, 'h')"
    done
}

function select1()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts FORMAT Null"
    done
}

function select2()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        ${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.asynchronous_inserts FORMAT Null"
    done
}

function flush1()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        sleep 0.2
        ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
    done
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

TIMEOUT=10

export -f insert1
export -f insert2
export -f insert3
export -f select1
export -f select2
export -f flush1

for _ in {1..5}; do
    insert1 $TIMEOUT &
    insert2 $TIMEOUT &
    insert3 $TIMEOUT &
done

select1 $TIMEOUT &
select2 $TIMEOUT &
flush1 $TIMEOUT &

wait

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.asynchronous_inserts WHERE database = currentDatabase() AND table = 'async_inserts'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts";
