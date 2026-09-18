#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The companion of `05059_adaptive_aggregator_pressure_valve_memory` for the fixed-width wide
# keys. The pressure sweeps size the batch they claim, and the finish drain the batches it
# converts, from the bytes a record is expected to cost in the destination table. That charge
# used to be one constant for every key kind - the 16-byte cell of a `UInt64` key at the fill the
# hash table keeps - while a `GROUP BY` over two `UInt64` columns drains into a `keys128` table
# whose cell is 24 bytes, and one over four into a `keys256` table whose cell is 40, so those
# tables came out wider than the claim that sized them. The charge now comes from the drain
# table's own variant; these two queries pin the two fixed-width kinds, each with a state that is
# narrower than its key so the cell is the widest thing the estimate has to see.
#
# All-distinct keys keep every producer frozen: nothing repeats, so the thaw verdict cannot fire
# and the whole stream goes through the staging path, which is where the valve runs. The
# thresholds are pinned per query because the runner randomizes them, and `max_memory_usage` is
# the cell that encodes the claim: ten times the threshold the query asked to spill at, which a
# valve sized from that threshold stays well inside.
#
# Each query runs in its own clickhouse-local process, so the counters in `system.events` belong
# to it alone.
for keys in "number AS a, number * 7 AS b" "number AS a, number * 7 AS b, number * 13 AS c, number * 17 AS d"; do
    $CLICKHOUSE_LOCAL --query "
    SET enable_adaptive_aggregator = 1;
    SET adaptive_aggregator_freeze_threshold = 1000;
    SET adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 1000;
    SET group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 20000000;
    SET max_bytes_ratio_before_external_group_by = 0;
    SET max_memory_usage = 300000000;
    SET max_threads = 4;
    SET max_block_size = 8192;

    SELECT count(), sum(cnt) FROM (SELECT ${keys}, count() AS cnt FROM numbers_mt(3000000) GROUP BY ALL);

    SELECT 'went external', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
    SELECT 'the valve ran', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureSweeps') > 0;
    SELECT 'the tables froze', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
    SELECT 'the drain carried the stream', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureDrainedRecords') > 2000000;
    SELECT 'stayed on the frozen path',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0
        AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPressureStandDowns') = 0;
    "
done
