#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

function thread()
{
    db_engine=`$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.databases WHERE name='$CLICKHOUSE_DATABASE'"`
    if [[ $db_engine == "Atomic" ]]; then
        # Ignore "Replica already exists" exception
        while true; do
            $CLICKHOUSE_CLIENT -n -q "DROP TABLE IF EXISTS test_table_$1 NO DELAY;
                CREATE TABLE test_table_$1 (a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01305/alter_table', 'r_$1') ORDER BY tuple();" 2>&1 |
                    grep -vP '(^$)|(^Received exception from server)|(^\d+\. )|because the last replica of the table was dropped right now|is already started to be removing by another replica right now|is already finished removing by another replica right now|Removing leftovers from table|Another replica was suddenly created|was successfully removed from ZooKeeper|was created by another server at the same moment|was suddenly removed|some other replicas were created at the same time|already exists'
            done
    else
        while true; do
            $CLICKHOUSE_CLIENT -n -q "DROP TABLE IF EXISTS test_table_$1;
                CREATE TABLE test_table_$1 (a UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01305/alter_table', 'r_$1') ORDER BY tuple();" 2>&1 |
                    grep -vP '(^$)|(^Received exception from server)|(^\d+\. )|because the last replica of the table was dropped right now|is already started to be removing by another replica right now|is already finished removing by another replica right now|Removing leftovers from table|Another replica was suddenly created|was successfully removed from ZooKeeper|was created by another server at the same moment|was suddenly removed|some other replicas were created at the same time'
            done
    fi
}


# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f thread;

TIMEOUT=10

timeout $TIMEOUT bash -c 'thread 1' &
timeout $TIMEOUT bash -c 'thread 2' &

wait

for i in {1,2}; do $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table_$i"; done
