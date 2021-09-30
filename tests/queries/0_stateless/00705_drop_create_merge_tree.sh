#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function stress()
{
    # We set up a signal handler to make sure to wait for all queries to be finished before ending
    CONTINUE=true
    handle_interruption()
    {
        CONTINUE=false
    }
    trap handle_interruption INT

    while $CONTINUE; do
        ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS table (x UInt8) ENGINE = MergeTree ORDER BY tuple()" 2>/dev/null
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE table" 2>/dev/null
    done

    trap - INT
}

# https://stackoverflow.com/questions/9954794/execute-a-shell-function-with-timeout
export -f stress

for _ in {1..5}; do
    # Ten seconds are just barely enough to reproduce the issue in most of runs.
    timeout -s INT 10 bash -c stress &
done

wait
echo

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS table";
