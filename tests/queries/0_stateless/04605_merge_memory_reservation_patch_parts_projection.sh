#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path forced by patch parts. A merge that applies patch parts (apply_patches_on_merge)
# may reduce rows, so MergeTask::prepareProjectionsToMergeAndRebuild REBUILDS every projection from the merged
# rows instead of merging the source projection parts - even when every base part already has the projection.
# Before the fix the estimate still priced a fully-present projection as a nested merge of the stale source
# projection parts, so a patch that adds a new JSON path or expands a projection expression was invisible to the
# reservation. The fix also sizes the read-back for the multiple temporary projection parts that
# MergeProjectionPartsTask merges at once (up to max_parts_to_merge_in_one_level), not a single reader set; a
# small min_insert_block_size_rows makes the rebuild squash into several temporary projection parts so that
# multi-part read-back path is actually driven. OPTIMIZE reserves memory unconditionally, so this is a coverage
# test - it must still succeed under a pathologically small soft limit, not assert on the estimate's numeric
# value - but it exercises the patch-forced projection rebuild code path added by the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    SET enable_lightweight_update = 1;

    CREATE TABLE t_merge_mem_patch_proj
    (
        k UInt64,
        v UInt64,
        j JSON,
        PROJECTION p_order (SELECT k, v ORDER BY v),
        PROJECTION p_agg (SELECT k % 10, sum(v) GROUP BY k % 10)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1,
        min_bytes_for_wide_part = 0;

    -- Three parts, each materializing both projections, so without patches the merge would MERGE the projection
    -- parts; the patch below forces a rebuild of the fully-present projections instead.
    SYSTEM STOP MERGES t_merge_mem_patch_proj;
    INSERT INTO t_merge_mem_patch_proj SELECT number, number * 2, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_patch_proj SELECT number, number * 2, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_patch_proj SELECT number, number * 2, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_patch_proj;

    -- The patch introduces a JSON path ('patch_only') that no base part - nor its projection parts - ever stored.
    UPDATE t_merge_mem_patch_proj SET j = ('{\"a\": ' || toString(k) || ', \"patch_only\": ' || toString(k) || '}')::JSON WHERE k % 2 = 0;

    -- min_insert_block_size_rows keeps each rebuilt projection batch small, so the rebuild writes several
    -- temporary projection parts and MergeProjectionPartsTask reads them back through more than one reader.
    OPTIMIZE TABLE t_merge_mem_patch_proj FINAL
        SETTINGS optimize_throw_if_noop = 1, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0;

    SELECT count(), countIf(toJSONString(j) LIKE '%patch_only%') FROM t_merge_mem_patch_proj SETTINGS apply_patch_parts = 1;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_patch_proj' AND active AND partition_id = 'all';
    -- Both projections must survive the patch-forced rebuild in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_patch_proj' AND active
        ORDER BY name;
    -- And they must still answer queries correctly after the rebuild.
    SELECT sum(v) FROM (SELECT sum(v) AS v FROM t_merge_mem_patch_proj GROUP BY k % 10);
" -- --merges_mutations_memory_usage_soft_limit=1
