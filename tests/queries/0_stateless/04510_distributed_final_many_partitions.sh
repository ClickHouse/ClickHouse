#!/usr/bin/env bash
# Tags: no-darwin, no-old-analyzer, no-flaky-check, no-distributed-cache
# no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
# no-old-analyzer: make_distributed_plan requires the analyzer.
# no-flaky-check: creating 600 parts across 300 partitions takes seconds on debug and sanitizer
# builds; the flaky check's repeated runs exceed its budget.
# no-distributed-cache: with the distributed cache each tiny part commit costs over a second on
# sanitizer builds, so the 600 inserted parts alone exceed the test time limit.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A per-partition FINAL split creates at least one primary-key-range layer per partition, so a
# table with many partitions produces more layers than the planner asks for. The read must still
# distribute: the layers are grouped into the target task count, several lanes per task, the same
# way a single-node per-partition FINAL builds one merge pipe per partition. The plan shape does
# not expose the split, so the check reads the planner's trace message.

$CLICKHOUSE_CLIENT --multiquery -q "
SET max_partitions_per_insert_block = 150;
-- Statistics play no role here, and writing a statistics blob for each of the 300 tiny parts
-- makes the setup exceed the test time limit on sanitizer builds with object storage.
SET materialize_statistics_on_insert = 0;
DROP TABLE IF EXISTS t_final_many_partitions;
CREATE TABLE t_final_many_partitions (k UInt64, p UInt16, v UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver)
PARTITION BY p ORDER BY k SETTINGS index_granularity = 64, auto_statistics_types = '';
SYSTEM STOP MERGES t_final_many_partitions;
-- 150 partitions, far more than the 4 tasks x 16 layers per task the planner aims for. Every key
-- appears in every partition and must survive per-partition dedup, and its newer version must win.
INSERT INTO t_final_many_partitions SELECT number % 100, intDiv(number, 100), number, 1 FROM numbers(15000);
INSERT INTO t_final_many_partitions SELECT number % 100, intDiv(number, 100), number + 5, 2 FROM numbers(15000);
"

SETTINGS="enable_parallel_replicas = 0, max_rows_to_group_by = 0,
    distributed_plan_default_reader_bucket_count = 4, do_not_merge_across_partitions_select_final = 1"

echo -n "split groups all partitions into the target tasks "
if $CLICKHOUSE_CLIENT --send_logs_level=trace -q "
    SELECT count(), sum(v) FROM t_final_many_partitions FINAL FORMAT Null
    SETTINGS make_distributed_plan = 1, $SETTINGS" 2>&1 \
    | grep -q "Distributed FINAL read bucketed: 150 layers in 38 lanes per task make 4 tasks"
then echo 1; else echo 0; fi

# Both plans must return identical results.
echo -n "distributed plan "
$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(v) FROM t_final_many_partitions FINAL
    SETTINGS make_distributed_plan = 1, $SETTINGS"
echo -n "plain plan "
$CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(v) FROM t_final_many_partitions FINAL
    SETTINGS make_distributed_plan = 0, $SETTINGS"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_final_many_partitions"
