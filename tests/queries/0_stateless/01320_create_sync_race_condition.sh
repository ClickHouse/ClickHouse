#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS r;"

function thread1()
{
    while true; do $CLICKHOUSE_CLIENT -n --query "CREATE TABLE r (x UInt64) ENGINE = ReplicatedMergeTree('/test/table', 'r') ORDER BY x; DROP TABLE r;"; done
}

function thread2()
{
    while true; do $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA r" 2>/dev/null; done
}

export -f thread1
export -f thread2

timeout 10 bash -c thread1 &
timeout 10 bash -c thread2 &

wait

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS r;"
