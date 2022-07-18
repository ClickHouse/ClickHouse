#!/usr/bin/env bash
# Tags: no-random-settings

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


function insert1()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"
    while true; do
        ${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT CSV
1,"a"
2,"b"
'
    done
}

function insert2()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"
    while true; do
        ${CLICKHOUSE_CURL} -sS "$url" -d 'INSERT INTO async_inserts FORMAT JSONEachRow {"id": 5, "s": "e"} {"id": 6, "s": "f"}'
    done
}

function insert3()
{
    url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0"
    while true; do
        ${CLICKHOUSE_CURL} -sS "$url" -d "INSERT INTO FUNCTION remote('127.0.0.1', $CLICKHOUSE_DATABASE, async_inserts) VALUES (7, 'g') (8, 'h')"
    done
}

function select1()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM async_inserts FORMAT Null"
}

function select2()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM system.asynchronous_inserts FORMAT Null"
}

function truncate1()
{
    sleep 0.1
    ${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE async_inserts"
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE async_inserts (id UInt32, s String) ENGINE = MergeTree ORDER BY id"

TIMEOUT=10

export -f insert1
export -f insert2
export -f insert3
export -f select1
export -f select2
export -f truncate1

for _ in {1..5}; do
    timeout $TIMEOUT bash -c insert1 &
    timeout $TIMEOUT bash -c insert2 &
    timeout $TIMEOUT bash -c insert3 &
done

clickhouse_client_loop_timeout $TIMEOUT select1 &
clickhouse_client_loop_timeout $TIMEOUT select2 &
clickhouse_client_loop_timeout $TIMEOUT truncate1 &

wait
echo "OK"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS async_inserts";
