#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS buffer_00763_2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE buffer_00763_2 (s String) ENGINE = Buffer('$CLICKHOUSE_DATABASE', mt_00763_2, 1, 1, 1, 1, 1, 1, 1)"


function thread1()
{
    seq 1 500 | sed -r -e 's/.+/DROP TABLE IF EXISTS mt_00763_2; CREATE TABLE mt_00763_2 (s String) ENGINE = MergeTree ORDER BY s; INSERT INTO mt_00763_2 SELECT toString(number) FROM numbers(10);/' | ${CLICKHOUSE_CLIENT} --multiquery --ignore-error ||:
}

function thread2()
{
    seq 1 1000 | sed -r -e 's/.+/SELECT count() FROM buffer_00763_2;/' | ${CLICKHOUSE_CLIENT} --multiquery --server_logs_file='/dev/null' --ignore-error 2>&1 | grep -vP '^0$|^10$|^Received exception|^Code: 60|^Code: 218|^Code: 473'
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE buffer_00763_2"
