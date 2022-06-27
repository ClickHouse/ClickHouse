#!/usr/bin/env bash
# Tags: race, no-parallel

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# This test reproduces "Directory not empty" error in DROP DATABASE query.

function thread1()
{
    # ${CLICKHOUSE_CLIENT} --query="SHOW TABLES FROM test_01444"
    ${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_01444" 2>&1| grep -F "Code: " | grep -Fv "Code: 219"
    ${CLICKHOUSE_CLIENT} --query="CREATE DATABASE IF NOT EXISTS test_01444"
}

function thread2()
{
    ${CLICKHOUSE_CLIENT} --query="CREATE TABLE IF NOT EXISTS test_01444.t$RANDOM (x UInt8) ENGINE = MergeTree ORDER BY tuple()" 2>/dev/null
}

export -f thread1
export -f thread2

TIMEOUT=10

clickhouse_client_loop_timeout $TIMEOUT thread1 &
clickhouse_client_loop_timeout $TIMEOUT thread2 &
clickhouse_client_loop_timeout $TIMEOUT thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS test_01444" 2>&1| grep -F "Code: " | grep -Fv "Code: 219" || exit 0
