#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
# - no-random-merge-tree-settings -- may change number of parts

# There will be warnings in logs for unavailable replicas that we have in parallel_replicas cluster.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test that distributed index analysis works correctly when PK contains
# expressions over columns (e.g. toStartOfMinute(timestamp)), not just
# plain column references.

$CLICKHOUSE_CLIENT -nm -q "
  drop table if exists test_pk_expr;
  create table test_pk_expr (timestamp DateTime64(9), value Int)
    engine=MergeTree()
    order by toStartOfMinute(timestamp)
    settings
      distributed_index_analysis_min_parts_to_activate=0,
      distributed_index_analysis_min_indexes_bytes_to_activate=0;
  system stop merges test_pk_expr;
  insert into test_pk_expr select toDateTime64('2024-01-01 00:00:00', 9) + number, number from numbers(1e6)
    settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
  select count() from system.parts where database = currentDatabase() and table = 'test_pk_expr' and active;
"

function jq_pk_filter()
{
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | {
    "Type": .Type,
    "Condition": .Condition,
    "Initial Parts": ."Initial Parts",
    "Selected Parts": ."Selected Parts",
    "Initial Granules": ."Initial Granules",
    "Selected Granules": ."Selected Granules"
  }'
}

echo "distributed_index_analysis=0"
$CLICKHOUSE_CLIENT --format=LineAsString -q "
  explain indexes=1, json=1
  select * from test_pk_expr
  where timestamp = toDateTime64('2024-01-01 00:05:00', 9)
" | jq_pk_filter

echo "distributed_index_analysis=1"
explain_opts=(
  --format=LineAsString
  --cluster_for_parallel_replicas=parallel_replicas
  --distributed_index_analysis=1
  --max_parallel_replicas=11
)
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "
  explain indexes=1, json=1
  select * from test_pk_expr
  where timestamp = toDateTime64('2024-01-01 00:05:00', 9)
" | jq_pk_filter
