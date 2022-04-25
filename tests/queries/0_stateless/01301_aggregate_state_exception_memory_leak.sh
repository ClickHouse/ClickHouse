#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# If the memory leak exists, it will lead to OOM fairly quickly.
clickhouse_client_loop_timeout 30 $CLICKHOUSE_CLIENT --max_memory_usage 1G -q 'SELECT uniqExactState(number) FROM system.numbers_mt GROUP BY number % 10' 2>&1 | grep -o -F 'Memory limit (for query) exceeded' | uniq
echo 'Ok'
