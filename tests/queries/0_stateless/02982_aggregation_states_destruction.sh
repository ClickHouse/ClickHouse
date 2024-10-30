#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


query_id="02982_$RANDOM"
$CLICKHOUSE_CLIENT --query_id $query_id --log_query_threads 1 --query="select number, uniq(number) from numbers_mt(1e7) group by number limit 100 format Null;"

$CLICKHOUSE_CLIENT -q "system flush logs;"

$CLICKHOUSE_CLIENT -q "select count() > 1 from system.query_thread_log where query_id = '$query_id' and current_database = currentDatabase() and thread_name = 'AggregDestruct';"
