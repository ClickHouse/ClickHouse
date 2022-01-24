#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function test()
{
    for _ in {1..1000}; do
        $CLICKHOUSE_CLIENT --max_memory_usage 1G <<< "SELECT uniqExactState(number) FROM system.numbers_mt GROUP BY number % 10";
    done
}

export -f test;

# If the memory leak exists, it will lead to OOM fairly quickly.
timeout 30 bash -c test 2>&1 | grep -o -F 'Memory limit (for query) exceeded' | uniq
echo 'Ok'
