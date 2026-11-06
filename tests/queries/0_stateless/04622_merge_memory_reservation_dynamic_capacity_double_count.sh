#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the type-capacity fallback for Dynamic columns (countDynamicCapacityStreams). The default serialization of a
# Dynamic node always enumerates its DynamicStructure stream, so countColumnStreams - which every caller adds on
# top of the capacity - already charges it; the capacity must therefore NOT count DynamicStructure again, or
# every fallback-priced Dynamic node (top-level or nested in a Tuple / Array / Map) would over-reserve one full
# writer stream, which on object storage is another multipart ceiling per column and can re-introduce the
# concurrent-merge starvation this PR removes. This exercises exactly that path: several Dynamic columns, some
# top-level and some nested, on COMPACT sources with no recorded substreams (write_marks_for_substreams_in_compact_parts = 0),
# so the estimate falls back to the declared type's write-time capacity at selection time. Under a pathologically
# small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, must
# merge everything down to a single Wide part, must not error while estimating, and the Dynamic data must survive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 keeps the bytes condition from forcing Compact; min_rows_for_wide_part = 2000
    -- then makes each 1000-row insert a Compact part while the 3000-row merged part is Wide.
    -- write_marks_for_substreams_in_compact_parts = 0 makes the compact parts record no substreams, so the
    -- Dynamic columns' streams are invisible to columns_substreams.txt at selection time and the estimate takes
    -- the type-capacity fallback (countDynamicCapacityStreams) whose Dynamic-node double count this fixes.
    CREATE TABLE t_merge_mem_dynamic_capacity
    (
        k UInt64,
        d1 Dynamic(max_types = 4),
        d2 Dynamic(max_types = 8),
        t Tuple(a UInt64, dyn Dynamic(max_types = 4)),
        arr Array(Dynamic(max_types = 4))
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 2000, write_marks_for_substreams_in_compact_parts = 0;

    SYSTEM STOP MERGES t_merge_mem_dynamic_capacity;
    INSERT INTO t_merge_mem_dynamic_capacity SELECT number,
        number::Dynamic, toString(number)::Dynamic,
        tuple(number, number::Dynamic), [number::Dynamic, toString(number)::Dynamic]
        FROM numbers(1000);
    INSERT INTO t_merge_mem_dynamic_capacity SELECT number,
        number::Dynamic, toString(number)::Dynamic,
        tuple(number, number::Dynamic), [number::Dynamic, toString(number)::Dynamic]
        FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_dynamic_capacity SELECT number,
        number::Dynamic, toString(number)::Dynamic,
        tuple(number, number::Dynamic), [number::Dynamic, toString(number)::Dynamic]
        FROM numbers(2000, 1000);

    -- The source parts must be Compact for the type-capacity fallback to be exercised.
    SELECT DISTINCT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_dynamic_capacity' AND active;

    SYSTEM START MERGES t_merge_mem_dynamic_capacity;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_dynamic_capacity FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_dynamic_capacity;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dynamic_capacity' AND active;
    -- The merged part is Wide, so countOutputStreams and the compact-source type-capacity fallback ran during
    -- selection with all four Dynamic columns behind it.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dynamic_capacity' AND active;
    -- The Dynamic data must survive the merge intact.
    SELECT sum(d1::UInt64) FROM t_merge_mem_dynamic_capacity;
" -- --merges_mutations_memory_usage_soft_limit=1
