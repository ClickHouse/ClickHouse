#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.mt"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS test.buffer"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.buffer (s String) ENGINE = Buffer(test, mt, 1, 1, 1, 1, 1, 1, 1)"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test.mt (x UInt32, s String) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --query="INSERT INTO test.mt VALUES (1, '1'), (2, '2'), (3, '3')"

function thread1()
{
    seq 1 300 | sed -r -e 's/.+/ALTER TABLE test.mt MODIFY column s UInt32; ALTER TABLE test.mt MODIFY column s String;/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error ||:
}

function thread2()
{
    seq 1 2000 | sed -r -e 's/.+/SELECT sum(length(s)) FROM test.buffer;/' | ${CLICKHOUSE_CLIENT} --multiquery --server_logs_file='/dev/null' --ignore-error 2>&1 | grep -vP '^3$'
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE test.mt"
${CLICKHOUSE_CLIENT} --query="DROP TABLE test.buffer"
