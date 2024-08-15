#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

while :; do
    $CLICKHOUSE_CLIENT -q "select sleepEachRow(0.1) from numbers(100) settings max_block_size=1 format Null" &
    pid=$!
    sleep 1.5
    duration="$($CLICKHOUSE_CLIENT -q "select floor(elapsed) from system.processes where current_database = currentDatabase() and query not like '%system.processes%'")"
    kill -INT $pid
    wait
    $CLICKHOUSE_CLIENT -q "kill query where current_database = currentDatabase() sync format Null"
    if [[ $duration -eq 1 ]]; then
        echo "OK"
        break
    fi
done
