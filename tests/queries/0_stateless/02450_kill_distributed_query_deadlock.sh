#!/usr/bin/env bash
# Tags: long, no-random-settings, no-debug

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that runs a distributed query and cancels it ASAP,
# this has a chance to trigger a hung/deadlock in ProcessorList.
for i in {1..50}
do
    query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$i"
    $CLICKHOUSE_CLIENT --format Null --query_id "$query_id" --max_rows_to_read 0 --max_bytes_to_read 0 --max_result_rows 0 --max_result_bytes 0 -q "select * from remote('127.{1|2|3|4|5|6}', numbers(1e12))" 2>/dev/null &
    while true
    do
        killed_queries="$($CLICKHOUSE_CLIENT -q "kill query where query_id = '$query_id' sync" | wc -l)"
        if [[ "$killed_queries" -ge 1 ]]
        then
            break
        fi
    done
    wait -n
    query_return_status=$?
    if [[ $query_return_status -eq 0 ]]
    then
        echo "Query $query_id should be cancelled, however it returns successfully"
    fi
done
