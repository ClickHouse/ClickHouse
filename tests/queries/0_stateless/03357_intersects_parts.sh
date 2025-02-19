#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t0 SYNC;

    CREATE TABLE t0 ( id UInt64 )
    ENGINE = MergeTree()
    ORDER BY id;
"

function insert1()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO t0 VALUES (1), (2), (3)"
        sleep 0.05
    done
}

function insert2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "INSERT INTO t0 VALUES (4), (5), (6)"
        sleep 0.05
    done
}

function insert_with_TX()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "
            BEGIN TRANSACTION;
            INSERT INTO t0 VALUES (7), (8), (9);
            ROLLBACK;
        "
        sleep 0.05
    done
}


function detach_attach()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "DETACH TABLE  t0;"
        $CLICKHOUSE_CLIENT --query "ATTACH TABLE  t0;"
        sleep 0.05
    done
}

export -f insert1;
export -f insert2;
export -f insert_with_TX;
export -f detach_attach;

TIMEOUT=60

for _ in {0..4}; do
    timeout $TIMEOUT bash -c insert1 2> /dev/null &
    timeout $TIMEOUT bash -c insert2 2> /dev/null &
    timeout $TIMEOUT bash -c insert_with_TX 2> /dev/null &
    timeout $TIMEOUT bash -c detach_attach 2> /dev/null &
done

wait

$CLICKHOUSE_CLIENT --query "
    SELECT count(distinct id) FROM t0;
    DROP TABLE IF EXISTS t0 SYNC;
"
