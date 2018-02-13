#!/usr/bin/env bash

if $CLICKHOUSE_CLIENT -q "select length(groupArray(number)) FROM (SELECT * FROM system.numbers LIMIT 1000000)" --compile=1 --min_count_to_compile=0 --max_threads=1 --max_memory_usage=8000000 2>/dev/null ; then
    echo 'There is no expected exception "Memory limit (for query) exceeded: would use..."'
fi
$CLICKHOUSE_CLIENT -q "SELECT 1"
