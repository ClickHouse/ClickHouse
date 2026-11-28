#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# when COMPACT source parts with no recorded substreams carry a Dynamic column whose single materialized variant
# is a wide composite - a named Tuple whose serialization opens one stream per element, more than the fixed
# per-variant worst case (STREAMS_PER_DYNAMIC_VARIANT) the type-capacity fallback assumes. A compact part written
# with write_marks_for_substreams_in_compact_parts = 0 records nothing in columns_substreams.txt and stores every
# column in a single data.bin, so the variant's real stream layout is not recoverable at selection time and the
# estimate falls back to the declared type's write-time capacity; the width of a composite variant is runtime
# data, so this fallback can under-estimate such a column - the residual is covered by the reservation being a
# soft throttle that always admits a single merge (documented on STREAMS_PER_DYNAMIC_VARIANT). This test pins the
# user-visible contract of exactly that base-merge fallback: under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL over compact Dynamic(max_types = 1)
# sources with a wide tuple variant reserves unconditionally, must merge everything down to a single Wide part,
# must not error while estimating, and the tuple variant's data must survive the merge intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 keeps the bytes condition from forcing Compact; min_rows_for_wide_part = 2000
    -- then makes each 1000-row insert a Compact part while the 3000-row merged part is Wide.
    -- write_marks_for_substreams_in_compact_parts = 0 makes the compact parts record no substreams, so the
    -- Dynamic column's variant streams are invisible to columns_substreams.txt at selection time.
    CREATE TABLE t_merge_mem_wide_tuple_variant
    (
        k UInt64,
        d Dynamic(max_types = 1)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 2000, write_marks_for_substreams_in_compact_parts = 0;

    SYSTEM STOP MERGES t_merge_mem_wide_tuple_variant;
    -- Every row carries the same named-tuple type, so it is the single materialized variant of the
    -- Dynamic(max_types = 1) column, and its serialization opens one stream per tuple element (plus a null map
    -- and array offsets) - wider than the fixed per-variant worst case of the type-capacity fallback.
    INSERT INTO t_merge_mem_wide_tuple_variant SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(1000);
    INSERT INTO t_merge_mem_wide_tuple_variant SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_wide_tuple_variant SELECT number,
        CAST(tuple(number, toString(number), number / 7, [number, number + 1], if(number % 2 = 0, number, NULL), toDate('2026-01-01') + number % 365),
             'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)')
        FROM numbers(2000, 1000);

    -- The source parts must be Compact for the compact-recovery fallback to be exercised.
    SELECT DISTINCT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_wide_tuple_variant' AND active;

    SYSTEM START MERGES t_merge_mem_wide_tuple_variant;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_wide_tuple_variant FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_wide_tuple_variant;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_wide_tuple_variant' AND active;
    -- The merged part is Wide, so countOutputStreams (and the compact-source type-capacity fallback) ran during
    -- selection with a wide tuple variant behind the Dynamic column.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_wide_tuple_variant' AND active;
    -- The wide tuple variant must survive the merge intact.
    SELECT sum(CAST(d, 'Tuple(a UInt64, s String, f Float64, arr Array(UInt64), n Nullable(UInt64), dt Date)').a)
        FROM t_merge_mem_wide_tuple_variant;
" -- --merges_mutations_memory_usage_soft_limit=1
