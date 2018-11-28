#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mt"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.buffer"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.buffer (s String) ENGINE = Buffer(test, mt, 1, 1, 1, 1, 1, 1, 1)"


function thread1()
{
    seq 1 500 | sed -r -e 's/.+/DROP TABLE IF EXISTS test.mt; CREATE TABLE test.mt (s String) ENGINE = MergeTree ORDER BY s; INSERT INTO test.mt SELECT toString(number) FROM numbers(10);/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error ||:
}

function thread2()
{
    seq 1 1000 | sed -r -e 's/.+/SELECT count() FROM test.buffer;/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error 2>&1 | grep -vP '^0$|^10$|^Received exception|^Code: 60'
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mt"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.buffer"
