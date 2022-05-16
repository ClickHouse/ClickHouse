#!/usr/bin/env bash
# Tags: race, zookeeper, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS test_01320"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE test_01320 ENGINE=Ordinary"   # Different bahaviour of DROP with Atomic

function thread1()
{
    $CLICKHOUSE_CLIENT -n --query "CREATE TABLE test_01320.r (x UInt64) ENGINE = ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/table', 'r') ORDER BY x; DROP TABLE test_01320.r;"
}

function thread2()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM SYNC REPLICA test_01320.r" 2>/dev/null
}

export -f thread1
export -f thread2

clickhouse_client_loop_timeout 10 thread1 &
clickhouse_client_loop_timeout 10 thread2 &

wait

$CLICKHOUSE_CLIENT --query "DROP DATABASE test_01320" 2>&1 | grep -F "Code:" | grep -v "New table appeared in database being dropped or detached" || exit 0
