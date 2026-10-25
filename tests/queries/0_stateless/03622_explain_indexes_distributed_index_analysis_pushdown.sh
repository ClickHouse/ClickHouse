#!/usr/bin/env bash
# There will be warnings in logs for unavailable replicas that we have in parallel_replicas cluster.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

# Proper push down is supported only with analyzer
CLICKHOUSE_CLIENT_OPT="--allow_experimental_analyzer=1"

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate many parts (partitions) to ensure that all replicas will be chosen for distributed index analysis
# even failed replica (that is included into parallel_replicas), and ensure that the SELECT wont fail (parts should be analyzed locally).

$CLICKHOUSE_CLIENT -nm -q "
  drop table if exists test_1m;
  -- -min_bytes_for_wide_part -- wide parts are different (they respect index_granularity completely, unlike compact parts) -- FIXME
  -- -merge_selector_base = 1000 -- disable merges
  create table test_1m (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base = 1000, index_granularity=8192, index_granularity_bytes=10e9, min_bytes_for_wide_part=1e9, distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
  system stop merges test_1m;
  insert into test_1m select number, number*100 from numbers(1e6) settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
  select count() from system.parts where database = currentDatabase() and table = 'test_1m' and active;
"

echo "distributed_index_analysis=0"
$CLICKHOUSE_CLIENT --format=LineAsString -q "explain indexes=1, json=1 select * from (select * from test_1m) where key > 100000" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey")'
}

echo "distributed_index_analysis=1"
explain_opts=(
  --format=LineAsString
  --cluster_for_parallel_replicas=parallel_replicas
  --distributed_index_analysis=1
  --max_parallel_replicas=11
)
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from (select * from test_1m) where key > 100000" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
