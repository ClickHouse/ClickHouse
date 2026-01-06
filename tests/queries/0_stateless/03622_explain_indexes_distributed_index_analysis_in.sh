#!/usr/bin/env bash
# Tags: long

# FIXME: convert to .sql

# There will be warnings in logs for unavailable replicas that we have in parallel_replicas cluster.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

# Proper IN is supported only with analyzer
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
  create table test_1m (key Int, value Int) engine=MergeTree() order by key settings merge_selector_base = 1000, index_granularity=8192, index_granularity_bytes=10e9, min_bytes_for_wide_part=1e9;
  system stop merges test_1m;
  insert into test_1m select number, number*100 from numbers(1e6) settings max_block_size=10000, min_insert_block_size_rows=10000, max_insert_threads=1;
  select count(), sum(marks) from system.parts where database = currentDatabase() and table = 'test_1m' and active;
"

explain_opts=(
  --format=LineAsString
  --cluster_for_parallel_replicas=test_cluster_one_shard_two_replicas
  --distributed_index_analysis=1
  --max_parallel_replicas=2
  --use_query_condition_cache=0
  # parallel replicas changes explain output
  --allow_experimental_parallel_reading_from_replicas=0
)
echo "IN (1000-element set)"
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from (select * from test_1m) where key in (select key from test_1m where (key % 1000) = 0)" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
echo "GLOBAL IN (1000-element set)"
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from (select * from test_1m) where key global in (select key from test_1m where (key % 1000) = 0)" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
echo "IN (10-element set)"
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from (select * from test_1m) where key in (select * from numbers(1000, 10))" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
echo "GLOBAL IN (10-element set)"
$CLICKHOUSE_CLIENT "${explain_opts[@]}" -q "explain indexes=1, json=1 select * from (select * from test_1m) where key global in (select * from numbers(1000, 10))" | {
  jq '.. | objects | select(has("Indexes")) | .Indexes[]? | select(.Type == "PrimaryKey") | .Distributed |= sort_by(.Address)'
}
$CLICKHOUSE_CLIENT -q "
system flush logs query_log;
-- SKIP: current_database = $CLICKHOUSE_DATABASE
select normalizeQuery(replace(query, currentDatabase(), 'default')) from system.query_log where event_date >= yesterday() and log_comment like '%' || currentDatabase() || '%' and type = 'QueryStart' and not(has(databases, 'system')) and query_kind in ('Select', 'Explain') order by event_time_microseconds;
"
