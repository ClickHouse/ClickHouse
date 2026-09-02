#!/usr/bin/env bash
# Tags: long, no-object-storage, no-msan, no-asan, no-tsan, no-debug
# Some kind of stress test, it doesn't make sense to test in a non-release build

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE IF EXISTS buffer_00763_2"

${CLICKHOUSE_CLIENT} --query="CREATE TABLE buffer_00763_2 (s String) ENGINE = Buffer('$CLICKHOUSE_DATABASE', mt_00763_2, 1, 1, 1, 1, 1, 1, 1)"


function thread1()
{
    seq 1 500 | sed -r -e 's/.+/DROP TABLE IF EXISTS mt_00763_2; CREATE TABLE mt_00763_2 (s String) ENGINE = MergeTree ORDER BY s; INSERT INTO mt_00763_2 SELECT toString(number) FROM numbers(10);/' | ${CLICKHOUSE_CLIENT} --fsync-metadata 0 --ignore-error ||:
}

function thread2()
{
    seq 1 500 | sed -r -e 's/.+/SELECT count() FROM buffer_00763_2;/' | ${CLICKHOUSE_CLIENT} --server_logs_file='/dev/null' --ignore-error 2>&1 | grep -vP '^0$|^10$|^Received exception|^Code: 60|^Code: 218|^Code: 473' | grep -v '(query: '
}

thread1 &
thread2 &

wait

${CLICKHOUSE_CLIENT} --query="DROP TABLE mt_00763_2"
${CLICKHOUSE_CLIENT} --query="DROP TABLE buffer_00763_2"
