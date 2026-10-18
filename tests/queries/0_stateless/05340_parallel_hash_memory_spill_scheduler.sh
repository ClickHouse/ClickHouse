#!/usr/bin/env bash
# The memory spill scheduler switches a `parallel_hash` join to `GraceHashJoin`, and that switch itself spills.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The spill threshold is out of reach, so any spill comes from the scheduler. One build stream reads the right
# side as one block, so the scheduler first sees a non-empty join after the whole right side is in, and the join
# is not polled again after the switch: a switch that does not spill leaves nothing on disk.
settings=(
    --join_algorithm=parallel_hash --max_threads=1 --max_block_size=1000000
    --max_bytes_before_external_join=100Gi --max_bytes_ratio_before_external_join=0
    --grace_hash_join_initial_buckets=1 --collect_hash_table_stats_during_joins=0
    --max_rows_in_join=0 --max_bytes_in_join=0 --enable_parallel_replicas=0
    --query_plan_join_swap_table=false --query_plan_optimize_join_order_randomize=0
    --max_memory_usage=105Mi
)

query="SELECT count(), sum(t2.number)
FROM numbers(100) AS t1
INNER JOIN (SELECT number FROM numbers(600000)) AS t2 ON t1.number = t2.number"

$CLICKHOUSE_CLIENT "${settings[@]}" --explain_query_plan_default=legacy \
    -q "SELECT trim(explain) FROM (EXPLAIN actions = 1 $query) WHERE trim(explain) LIKE 'Algorithm%'"

output=$($CLICKHOUSE_CLIENT "${settings[@]}" --enable_adaptive_memory_spill_scheduler=1 \
    --print-profile-events --profile-events-delay-ms=-1 -q "$query" 2>&1)
echo "$output" | grep -E '^[0-9]'
echo "$output" | grep -oE '\[ 0 \] JoinSpillingHashJoinSwitchedToGraceJoin: [0-9]+' | sed 's/\[ 0 \] //'
echo "$output" | grep -qE '\[ 0 \] ExternalJoinUncompressedBytes: [1-9]' && echo 'ExternalJoinUncompressedBytes > 0'

# Four build streams share 16 right blocks, and the scheduler switches the join while they fill it. Every right row
# has a match, so a row lost converting the four slots changes the result.
parallel_settings=(
    --join_algorithm=parallel_hash --max_threads=4 --max_block_size=65536
    --max_bytes_before_external_join=100Gi --max_bytes_ratio_before_external_join=0
    --grace_hash_join_initial_buckets=1 --collect_hash_table_stats_during_joins=0
    --max_rows_in_join=0 --max_bytes_in_join=0 --enable_parallel_replicas=0
    --query_plan_join_swap_table=false --query_plan_optimize_join_order_randomize=0
    --max_memory_usage=120Mi
)

parallel_query="SELECT count(), sum(t2.number)
FROM numbers(1000000) AS t1
INNER JOIN (SELECT number FROM numbers(1000000)) AS t2 ON t1.number = t2.number"

$CLICKHOUSE_CLIENT "${parallel_settings[@]}" \
    -q "SELECT 'FillingRightJoinSide', count() FROM (EXPLAIN PIPELINE $parallel_query) WHERE trim(explain) = 'FillingRightJoinSide'"

output=$($CLICKHOUSE_CLIENT "${parallel_settings[@]}" --enable_adaptive_memory_spill_scheduler=1 \
    --print-profile-events --profile-events-delay-ms=-1 -q "$parallel_query" 2>&1)
echo "$output" | grep -E '^[0-9]'
echo "$output" | grep -oE '\[ 0 \] JoinSpillingHashJoinSwitchedToGraceJoin: [0-9]+' | sed 's/\[ 0 \] //'
echo "$output" | grep -qE '\[ 0 \] ExternalJoinUncompressedBytes: [1-9]' && echo 'ExternalJoinUncompressedBytes > 0'
