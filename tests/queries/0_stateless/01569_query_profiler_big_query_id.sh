#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

query_id="aggregating_merge_tree_simple_aggregate_function_string_query100_profile100_$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} --query="select sleep(1)" --query_id="$query_id" --query_profiler_real_time_period_ns=10000000
${CLICKHOUSE_CLIENT} --query="system flush logs"
${CLICKHOUSE_CLIENT} --query="select count(*) > 1 from system.trace_log where query_id = '$query_id'"

