#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a metadata-only widened SCALAR column. Adding a value to an Enum via ALTER ... MODIFY COLUMN is a
# metadata-only conversion: the source parts keep storing the old, narrower Enum type, so on the next merge the
# column's stored type differs from the merged metadata and the estimate prices it as a type-widened column -
# the full output footprint (static skeleton plus the type's write-time dynamic capacity, zero for an Enum) or
# the streams the source parts demonstrably wrote, whichever is larger. The skeleton must not be added on top of
# the source-visible streams (which already contain it): that would price this one-stream column at two streams
# and inflate the reservation of every schema-evolution merge. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, must merge
# everything down to a single part, and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_merge_mem_scalar_widen (k UInt64, e Enum8('old' = 1), v String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_scalar_widen;
    INSERT INTO t_merge_mem_scalar_widen SELECT number, 'old', toString(number) FROM numbers(1000);
    INSERT INTO t_merge_mem_scalar_widen SELECT number, 'old', toString(number) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_scalar_widen SELECT number, 'old', toString(number) FROM numbers(2000, 1000);

    -- A metadata-only widen: the parts keep the narrower Enum8('old' = 1), the metadata is now wider, so the
    -- next merge reserializes the column under the current type and the estimate takes the type-widened path.
    ALTER TABLE t_merge_mem_scalar_widen MODIFY COLUMN e Enum8('old' = 1, 'new' = 2);

    SYSTEM START MERGES t_merge_mem_scalar_widen;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_scalar_widen FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_scalar_widen;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_scalar_widen' AND active;
    -- The merged part stores the column under the widened type.
    SELECT type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_scalar_widen' AND active AND column = 'e';
    SELECT sum(k) FROM t_merge_mem_scalar_widen;
" -- --merges_mutations_memory_usage_soft_limit=1
