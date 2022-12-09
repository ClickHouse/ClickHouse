#!/usr/bin/env bash

# shellcheck disable=SC2154

unset CLICKHOUSE_LOG_COMMENT

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "create table t(ts DateTime64) engine=MergeTree order by ts as select * from numbers_mt(1e6);"

query_id="${CLICKHOUSE_DATABASE}_02499_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id="$query_id" -q "select ts from t order by toUnixTimestamp64Nano(ts) limit 10 format Null settings max_block_size = 8192, max_threads = 5, optimize_read_in_order = 1;"

$CLICKHOUSE_CLIENT -q "system flush logs;"
$CLICKHOUSE_CLIENT --param_query_id="$query_id" -q "select read_rows < 50000 from system.query_log where event_date >= yesterday() and query_id = {query_id:String} and type = 'QueryFinish';"

