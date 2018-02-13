#!/usr/bin/env bash

SETTINGS="--compile=1 --min_count_to_compile=0 --max_threads=1 --max_memory_usage=8000000"
if ! $CLICKHOUSE_CLIENT -q "SELECT length(groupArray(number)) FROM (SELECT * FROM system.numbers LIMIT 1000000)" $SETTINGS 2>&1 | grep -Fc 'Memory limit (for query) exceeded' ; then
    echo 'There is no expected exception "Memory limit (for query) exceeded: would use..."'
fi
$CLICKHOUSE_CLIENT -q "SELECT 1"
