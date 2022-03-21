#!/usr/bin/env bash
# Tags: replica, no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

function thread()
{
    $CLICKHOUSE_CLIENT -n -q "DROP TABLE IF EXISTS test_table_$1 SYNC;
        CREATE TABLE test_table_$1 (a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/alter_table', 'r_$1') ORDER BY tuple();" 2>&1 |
            grep -vP '(^$)|(^Received exception from server)|(^\d+\. )|because the last replica of the table was dropped right now|is already started to be removing by another replica right now| were removed by another replica|Removing leftovers from table|Another replica was suddenly created|was created by another server at the same moment|was suddenly removed|some other replicas were created at the same time|^\(query: '
}

export -f thread

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread 1 &
clickhouse_client_loop_timeout $TIMEOUT thread 2 &

wait

for i in {1,2}; do $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table_$i"; done
