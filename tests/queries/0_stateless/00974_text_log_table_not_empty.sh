#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="SELECT 6103"

for (( i=1; i <= 50; i++ ))
do

${CLICKHOUSE_CLIENT} --query="SYSTEM FLUSH LOGS text_log"
sleep 0.1;
# Override random settings that slow down the test
if [[ $($CLICKHOUSE_CLIENT --max_rows_to_read 0 --min_bytes_to_use_direct_io 0 --min_bytes_to_use_mmap_io 0 --page_cache_inject_eviction 0 --max_threads 0 --query="SELECT 1 FROM system.text_log WHERE position(system.text_log.message, 'SELECT 6103') > 0 AND event_date >= yesterday() LIMIT 1") == 1 ]]; then echo 1; exit; fi;

done;


