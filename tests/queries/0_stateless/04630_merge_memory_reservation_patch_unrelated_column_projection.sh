#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path when a patch part updates ONLY a column the projection does not read. A patch
# part stores just the updated columns for existing rows, so the rebuilt temporary projection part contains
# neither the patch's rows (patches never add rows) nor its bytes. Before the fix the temp-part sizing summed
# every part's rows_count over source AND patch parts and, when a part stored no projection-required column at
# all, fell back to the part's whole uncompressed size - so a lightweight update of a fat unrelated column could
# flip a genuinely Compact rebuilt projection to Wide and reserve one writer buffer per substream for data the
# temp part never writes, throttling unrelated merges under merges_mutations_memory_usage_soft_limit. The fix
# counts base-part rows once, counts a part's bytes only when it actually stores a required column, and the
# whole-part stand-in remains only for such parts without per-column sizes (Compact). OPTIMIZE reserves memory
# unconditionally, so this must still succeed under a pathologically small soft limit - a coverage test that
# drives exactly the patch-on-unrelated-column rebuild sizing.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    SET enable_lightweight_update = 1;

    -- Default min_bytes_for_wide_part (10 MiB): the projection's real data (k, v - a few KiB) rebuilds into a
    -- Compact temp part; only the buggy whole-patch-part fallback (the fat 'extra' column below) could push the
    -- format decision toward Wide.
    CREATE TABLE t_merge_mem_patch_unrelated
    (
        k UInt64,
        v UInt64,
        extra String,
        PROJECTION p_order (SELECT k, v ORDER BY v)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_patch_unrelated;
    INSERT INTO t_merge_mem_patch_unrelated SELECT number, number * 2, repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_merge_mem_patch_unrelated SELECT number, number * 2, repeat('x', 100) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_patch_unrelated SELECT number, number * 2, repeat('x', 100) FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_patch_unrelated;

    -- The patch stores only 'extra' (plus system columns) - no projection-required column - yet forces the merge
    -- below to REBUILD the fully-present projection (apply_patches_on_merge => merge may reduce rows).
    UPDATE t_merge_mem_patch_unrelated SET extra = repeat('y', 1000) WHERE k % 2 = 0;

    OPTIMIZE TABLE t_merge_mem_patch_unrelated FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), countIf(extra = repeat('y', 1000)) FROM t_merge_mem_patch_unrelated SETTINGS apply_patch_parts = 1;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_patch_unrelated' AND active AND partition_id = 'all';
    -- The projection must survive the patch-forced rebuild and still answer correctly.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_patch_unrelated' AND active;
    SELECT sum(v) FROM (SELECT v FROM t_merge_mem_patch_unrelated ORDER BY v);
" -- --merges_mutations_memory_usage_soft_limit=1
