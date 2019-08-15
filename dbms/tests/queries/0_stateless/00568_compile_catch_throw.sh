#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

SETTINGS="--compile=1 --min_count_to_compile=0 --max_threads=1 --max_memory_usage=8000000 --server_logs_file=/dev/null"
output=$($CLICKHOUSE_CLIENT -q "SELECT length(groupArray(number)) FROM (SELECT * FROM system.numbers LIMIT 1000000)" $SETTINGS 2>&1)

[[ $? -eq 0 ]] && echo "Expected non-zero RC"
if ! echo "$output" | grep -Fc -e 'Memory limit (for query) exceeded' -e 'Cannot compile code' ; then
    echo -e 'There is no expected exception "Memory limit (for query) exceeded: would use..." or "Cannot compile code..."' "Whereas got:\n$output"
fi

$CLICKHOUSE_CLIENT -q "SELECT 1"
