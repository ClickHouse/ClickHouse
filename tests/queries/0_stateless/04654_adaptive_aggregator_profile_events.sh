#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Runs one query per adaptive control path and then asserts the path's ProfileEvents fired.
# Everything runs in a single clickhouse-local process, so the counters in `system.events`
# belong to these queries alone.
$CLICKHOUSE_LOCAL --query "
SET max_threads = 4;
SET max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_adaptive_aggregator = 1;

-- High-cardinality stream: freezes, stages, coalesces, bypasses the local probe (the frozen
-- table holds almost none of the keys), and drains at the merge.
SELECT count() FROM (SELECT number AS g, count() AS c FROM numbers_mt(400000) GROUP BY g) FORMAT Null;

-- Repeat-dominated stream past the freeze: the thaw sampler fires and the tables thaw.
SELECT count() FROM (SELECT toUInt64(number % 20000) AS g, count() AS c FROM numbers_mt(3000000) GROUP BY g) FORMAT Null;

-- Too few distinct keys: the producers give up on freezing.
SELECT count() FROM (SELECT toUInt64(number % 50) AS g, sum(number) AS s FROM numbers_mt(400000) GROUP BY g) FORMAT Null;

-- Constant memory pressure: the sweeps drain the backlogs early.
SELECT count() FROM (SELECT number % 100000 AS g, count() AS c FROM numbers_mt(400000) GROUP BY g SETTINGS max_bytes_before_external_group_by = 1) FORMAT Null;

SELECT 'freezes', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes';
SELECT 'staged records', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords';
SELECT 'staged bytes', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationStagedBytes';
SELECT 'sealed chunks', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationSealedChunks';
SELECT 'merge-time drained records', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationDrainedRecords';
SELECT 'probe bypasses', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationProbeBypasses';
SELECT 'thaws', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationThaws';
SELECT 'give-ups', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationGiveUps';
SELECT 'pressure sweeps', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps';
SELECT 'pressure-drained records', coalesce(sum(value), 0) > 0 FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords';
"
