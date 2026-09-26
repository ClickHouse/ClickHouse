#!/usr/bin/env bash
# Tags: long, no-fasttest, no-sanitize-coverage, no-replicated-database

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function thread1()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --query "
            DROP TABLE IF EXISTS t_lwu_block_number SYNC;
            SET enable_lightweight_update = 1;

            CREATE TABLE t_lwu_block_number (id UInt64, c1 UInt64, c2 String)
            ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_lwu_block_number/', '1')
            ORDER BY id
            SETTINGS
                enable_block_number_column = 1,
                enable_block_offset_column = 1;

            INSERT INTO t_lwu_block_number VALUES (1, 100, 'aa') (2, 200, 'bb') (3, 300, 'cc');
            "

    done
}

function thread2()
{
    local TIMELIMIT=$((SECONDS+$1))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT --query "
                SET enable_lightweight_update = 1;
                UPDATE t_lwu_block_number SET c2 = 'xx' WHERE id = 1;
                UPDATE t_lwu_block_number SET c2 = 'aa' WHERE id = 2;
                " 2> /dev/null || true
    done
}

export -f thread1
export -f thread2

TIMEOUT=10

thread1 $TIMEOUT >/dev/null &
thread2 $TIMEOUT >/dev/null &

wait
