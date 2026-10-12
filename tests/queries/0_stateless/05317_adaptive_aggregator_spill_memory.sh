#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Over the external-aggregation threshold, a frozen producer of the adaptive aggregator writes its
# staged records to the session's spill streams, one per bucket, once it holds a share of the
# threshold, and frees them; the merge task of every bucket reads the bucket's stream back and merges
# it one unit of partitions at a time. The memory of the staged path therefore follows the threshold:
# the records a producer holds before it spills, the first chunks of its partitions, the buffers of
# the spill streams, which are sized from the threshold because all of them are open together, and
# per merge task one bucket's spilled records and one unit's table. The aggregate states exist only
# in the merge, so neither their width nor the heap memory they own outside the arenas weighs on the
# producers.
#
# Each shape stages its records in one of the three formats: general records with variable-width
# parts (string keys with a narrow argument, with a wide string argument, a wide key, and states that
# own heap memory), key records of wide fixed-width keys for `count`, and fixed-stride general
# records whose `uniqUpTo(100)` state is some thirty times wider than the record. Distinct keys, or
# keys repeated too little for the thaw verdict, keep every producer frozen, so the whole stream goes
# through the staging path. The thresholds are pinned because the runner randomizes them, and
# `max_memory_usage`, five times the threshold, is the cell that encodes the claim; the exact totals
# move if a record is lost or doubled across the spill.
#
# Each query runs in its own clickhouse-local process, so the counters in `system.events` belong to
# it alone.
function run_shape()
{
    local rows=$1 threads=$2 block_size=$3 query=$4
    $CLICKHOUSE_LOCAL --query "
    SET enable_adaptive_aggregator = 1;
    SET adaptive_aggregator_freeze_threshold = 1000;
    SET adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 1000;
    SET group_by_two_level_threshold_bytes = 1000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 20000000;
    SET max_bytes_ratio_before_external_group_by = 0;
    SET max_memory_usage = 100000000;
    SET max_threads = ${threads};
    SET max_block_size = ${block_size};

    ${query};

    SELECT 'the producers spilled most of the stream',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') * 2 > ${rows};
    SELECT 'stayed on the frozen path',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0
        AND (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationThaws') = 0;
    "
}

echo 'String key, narrow argument'
run_shape 3000000 4 8192 "SELECT count(), sum(u) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(number)) AS k, uniq(number % 11) AS u
    FROM numbers_mt(3000000) GROUP BY k)"

echo 'String key, wide string argument'
run_shape 1500000 4 8192 "SELECT count(), sum(u) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(number)) AS k,
        uniq(concat(repeat('wide-aggregate-argument-', 8), toString(number))) AS u
    FROM numbers_mt(1500000) GROUP BY k)"

echo 'Wide string key'
run_shape 1000000 4 8192 "SELECT count(), sum(u) FROM (
    SELECT concat('a-deliberately-long-group-key-that-stages-wide-', toString(number)) AS k, uniq(number % 7) AS u
    FROM numbers_mt(1000000) GROUP BY k)"

echo 'String key, states with heap memory'
run_shape 1000000 4 8192 "SELECT count(), sum(u), sum(b), sum(a) FROM (
    SELECT concat('an-ordinary-looking-group-key-', toString(intDiv(number, 4))) AS k,
        uniqExact(number) AS u,
        bitmapCardinality(groupBitmapState(number)) AS b,
        length(groupArray(number)) AS a
    FROM numbers_mt(1000000) GROUP BY k)"

echo 'Two UInt64 keys, count'
run_shape 3000000 4 8192 "SELECT count(), sum(cnt) FROM (
    SELECT number AS a, number * 7 AS b, count() AS cnt FROM numbers_mt(3000000) GROUP BY ALL)"

echo 'Four UInt64 keys, count'
run_shape 3000000 4 8192 "SELECT count(), sum(cnt) FROM (
    SELECT number AS a, number * 7 AS b, number * 13 AS c, number * 17 AS d, count() AS cnt FROM numbers_mt(3000000) GROUP BY ALL)"

echo 'UInt64 key, wide fixed state'
run_shape 1200000 2 65536 "SELECT count(), sum(u) FROM (
    SELECT number AS k, uniqUpTo(100)(number) AS u FROM numbers_mt(1200000) GROUP BY k)"
