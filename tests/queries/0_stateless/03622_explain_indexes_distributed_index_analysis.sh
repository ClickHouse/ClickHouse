#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings
# - no-random-merge-tree-settings -- may change number of parts

# There will be warnings in logs for unavailable replicas that we have in parallel_replicas cluster.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate many parts (partitions) to ensure that all replicas will be chosen for distributed index analysis
# even failed replica (that is included into parallel_replicas), and ensure that the SELECT wont fail (parts should be analyzed locally).

$CLICKHOUSE_CLIENT -nm -q "
  drop table if exists test_10m;
  create table test_10m (key Int, value Int) engine=MergeTree() order by key settings distributed_index_analysis_min_parts_to_activate=0, distributed_index_analysis_min_indexes_bytes_to_activate=0;
  system stop merges test_10m;
  insert into test_10m select number, number*100 from numbers(1e6) settings max_partitions_per_insert_block=100, max_block_size=10000, min_insert_block_size_rows=10000;
  select count() from system.parts where database = currentDatabase() and table = 'test_10m';
"

echo "distributed_index_analysis=0"
$CLICKHOUSE_CLIENT --format=LineAsString -q "explain indexes=1, json=1 select * from test_10m where key > 100000" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey")'
}

echo "distributed_index_analysis=1"
explain_opts=(
  --format=LineAsString
  --cluster_for_parallel_replicas=parallel_replicas
  --distributed_index_analysis=1
  --max_parallel_replicas=11
)
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from test_10m where key > 100000" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
