#!/usr/bin/env bash
# Tags: no-old-analyzer

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The memory spill scheduler spills the right side of a block nested loop join whose own spill thresholds
# are off, without changing the result. The single 32 MB right block reserves twice its size for a spill,
# more than `max_memory_usage`, so the scheduler always asks for it.

settings="join_algorithm = 'partial_merge', query_plan_join_swap_table = 'false', max_threads = 1,
    max_block_size = 4000000, min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    cross_join_min_rows_to_compress = 0, cross_join_min_bytes_to_compress = 0, max_memory_usage = '60Mi'"

query="SELECT count(), sum(r.y) FROM (SELECT number AS x FROM numbers(3)) l
    LEFT JOIN (SELECT number AS y FROM numbers(4000000)) r ON r.y > l.x + 3999990"

$CLICKHOUSE_CLIENT --query "SELECT count() FROM (EXPLAIN $query) WHERE explain LIKE '%BlockNestedLoopJoin%' SETTINGS $settings"

exec 3>&1
for scheduler in 0 1; do
    echo "enable_adaptive_memory_spill_scheduler = $scheduler"
    events=$($CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 \
        --query "$query SETTINGS $settings, enable_adaptive_memory_spill_scheduler = $scheduler" 2>&1 >&3)
    echo "spilled: $(grep -c '\] ExternalJoinWritePart: [1-9]' <<< "$events")"
done
