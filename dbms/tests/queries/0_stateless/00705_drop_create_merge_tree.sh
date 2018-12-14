#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

function stress()
{
    while true; do
        ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS test.table (x UInt8) ENGINE = MergeTree ORDER BY tuple()" 2>/dev/null
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE test.table" 2>/dev/null
    done
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f stress

for thread in {1..5}; do
    # Ten seconds are just barely enough to reproduce the issue in most of runs.
    timeout 10 bash -c stress &
done

wait
echo

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.table";
