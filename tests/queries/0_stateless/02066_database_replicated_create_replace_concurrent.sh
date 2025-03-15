#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT -q "CREATE DATABASE rdb_$CLICKHOUSE_DATABASE ENGINE=Replicated('/test/02066/rdb/$CLICKHOUSE_DATABASE', '{shard}', '{replica}')"

function thread1()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "CREATE OR REPLACE TABLE rdb_$CLICKHOUSE_DATABASE.t$(($RANDOM % 3)) (n int) ENGINE=Memory";
    done
}

function thread2()
{
    while true; do
        $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT database_replicated_rename_table_session_expired_before_commit";
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT database_replicated_rename_table_session_expired_before_commit";
        $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT database_replicated_rename_table_session_expired_after_commit";
        sleep 0.$RANDOM
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT database_replicated_rename_table_session_expired_after_commit";
        sleep 0.$RANDOM
    done
}

export -f thread1;
export -f thread2;

TIMEOUT=30

timeout $TIMEOUT bash -c thread2 2> /dev/null &

timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread1 2> /dev/null &
timeout $TIMEOUT bash -c thread1 2> /dev/null &

wait
