#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a merge that removes expired TTL values AND still runs the vertical algorithm.
# MergeTask::ExecuteAndFinalizeHorizontalPart::chooseMergeAlgorithm does not force Horizontal for every
# need_remove_expired_values merge: it does so only when MergeTask::canVerticalTTLDelete is false, so an Ordinary
# merge with a plain rows TTL (no GROUP BY / column TTL, no lightweight delete) under
# vertical_merge_optimize_ttl_delete keeps gathering its columns one at a time. The estimator used to drop out of
# the vertical pricing for any TTL merge and charge the full horizontal footprint - every output stream's buffers
# alive at once - which is the over-reservation/starvation pattern this estimate exists to avoid for wide merges on
# object storage. The estimator now mirrors canVerticalTTLDelete and prices only the streams a vertical merge keeps
# alive, with the TTL expression columns pulled into the merging set exactly as extractMergingAndGatheringColumns
# does. OPTIMIZE reserves unconditionally, so this must still succeed under a pathologically small soft limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_vertical_ttl
    (
        k UInt64,
        event_date Date,
        c1 String,
        c2 String,
        c3 String,
        c4 String,
        c5 String,
        c6 String,
        c7 String,
        c8 String
    )
    ENGINE = MergeTree ORDER BY k
    -- A plain rows TTL keyed on a column that is NOT part of the sorting key, so the TTL expression column has to
    -- be pulled into the merging (horizontal-stage) set by the estimator, and the eight payload columns are
    -- gathered one at a time.
    TTL event_date + INTERVAL 1 DAY
    SETTINGS min_bytes_for_wide_part = 0,
             vertical_merge_algorithm_min_rows_to_activate = 0,
             vertical_merge_algorithm_min_bytes_to_activate = 0,
             vertical_merge_algorithm_min_columns_to_activate = 1,
             merge_with_ttl_timeout = 0;

    SYSTEM STOP MERGES t_merge_mem_vertical_ttl;
    -- Half of the rows are already expired, so the merge really does remove expired values.
    INSERT INTO t_merge_mem_vertical_ttl
        SELECT number, if(number % 2 = 0, '2000-01-01'::Date, '2100-01-01'::Date),
               repeat('a', 40), repeat('b', 40), repeat('c', 40), repeat('d', 40),
               repeat('e', 40), repeat('f', 40), repeat('g', 40), repeat('h', 40)
        FROM numbers(1000);
    INSERT INTO t_merge_mem_vertical_ttl
        SELECT number, if(number % 2 = 0, '2000-01-01'::Date, '2100-01-01'::Date),
               repeat('a', 40), repeat('b', 40), repeat('c', 40), repeat('d', 40),
               repeat('e', 40), repeat('f', 40), repeat('g', 40), repeat('h', 40)
        FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_vertical_ttl
        SELECT number, if(number % 2 = 0, '2000-01-01'::Date, '2100-01-01'::Date),
               repeat('a', 40), repeat('b', 40), repeat('c', 40), repeat('d', 40),
               repeat('e', 40), repeat('f', 40), repeat('g', 40), repeat('h', 40)
        FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_vertical_ttl;

    OPTIMIZE TABLE t_merge_mem_vertical_ttl FINAL SETTINGS optimize_throw_if_noop = 1;

    -- Only the non-expired half survives, and every gathered column still carries its value.
    SELECT count(), countIf(c1 = repeat('a', 40) AND c8 = repeat('h', 40)) FROM t_merge_mem_vertical_ttl;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_vertical_ttl' AND active AND partition_id = 'all';
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_vertical_ttl' AND active AND partition_id = 'all';
    SELECT sum(k) FROM t_merge_mem_vertical_ttl;
" -- --merges_mutations_memory_usage_soft_limit=1
