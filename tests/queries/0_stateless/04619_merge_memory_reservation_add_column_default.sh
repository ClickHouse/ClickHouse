#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams / countRebuiltProjectionStreams) on the ALTER TABLE ... ADD COLUMN ... DEFAULT ... upgrade
# path for a semi-structured (JSON) column. Parts written before the ALTER do not store the column at all, yet the
# merge materializes it from the default expression for their rows (MergeTask keeps a missing column live while a
# default exists, and IMergeTreeReader fills and evaluates the missing defaults), so the merged part - and a rebuilt
# projection over that column - can write dynamic substreams that no source part records. The estimator must not
# treat the recorded per-part substream union as exact for such a column and must fall back to the output type's
# write-time capacity, both for the base output streams and for the bare-identifier rebuilt-projection pricing.
# Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything down to a single Wide part and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force every part (and the merged part) to be Wide,
    -- so the countOutputStreams per-substream / default-filled path is exercised rather than the compact one.
    -- deduplicate_merge_projection_mode = 'rebuild' lets OPTIMIZE ... DEDUPLICATE run on a table with projections
    -- by REBUILDING them from the merged rows - the rebuilt-projection pricing over the default-filled column.
    CREATE TABLE t_merge_mem_add_default
    (
        k UInt64
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, deduplicate_merge_projection_mode = 'rebuild';

    SYSTEM STOP MERGES t_merge_mem_add_default;
    -- Two wide parts written before the column exists: their rows are default-filled during the merge.
    INSERT INTO t_merge_mem_add_default SELECT number FROM numbers(1000);
    INSERT INTO t_merge_mem_add_default SELECT number FROM numbers(1000, 1000);

    -- Metadata-only: the old parts do not store d, but the default materializes real dynamic paths from k.
    ALTER TABLE t_merge_mem_add_default ADD COLUMN d JSON DEFAULT toJSONString(map('a', k, 'x', toString(k)));

    -- One more wide part written after the ALTER: it stores d physically with recorded substreams, so the
    -- per-part union is non-empty and would otherwise be trusted as exact.
    INSERT INTO t_merge_mem_add_default (k) SELECT number FROM numbers(2000, 1000);

    -- A bare-identifier projection over the default-filled column, added AFTER every insert so that no part
    -- stores it (parts with different projection sets cannot be merged): the deduplicating merge below
    -- rebuilds it from the merged rows regardless of presence (deduplicate_merge_projection_mode = 'rebuild').
    ALTER TABLE t_merge_mem_add_default ADD PROJECTION p (SELECT d, k ORDER BY k);

    -- The old parts really do lack d while the new one stores it, so the default-filled bailout is exercised.
    SELECT name, part_type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_add_default' AND active AND column = 'd'
        ORDER BY name;

    SYSTEM START MERGES t_merge_mem_add_default;

    -- A deduplicating merge rebuilds the projection from the merged rows (deduplicate_merge_projection_mode =
    -- 'rebuild'), after the missing defaults have been materialized. Must merge to a single part or throw,
    -- never no-op silently.
    OPTIMIZE TABLE t_merge_mem_add_default FINAL DEDUPLICATE SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_add_default;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_add_default' AND active;
    -- The merged part is Wide, so countOutputStreams (and the default-filled bailout) ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_add_default' AND active;
    -- The default-materialized rows survived the merge with their dynamic paths intact.
    SELECT sum(d.a::UInt64) FROM t_merge_mem_add_default;
" -- --merges_mutations_memory_usage_soft_limit=1
