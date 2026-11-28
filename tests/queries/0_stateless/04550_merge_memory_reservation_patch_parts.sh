#!/usr/bin/env bash
# Regression test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# ignoring future_part.patch_parts. A lightweight UPDATE creates a patch part; with apply_patches_on_merge = 1
# (the default) a merge of the base part also opens a reader for the patch part (MergeTreeReadTask::createReaders
# builds a separate MergeTreeReader per patch), and a patch that introduces a JSON path absent from every base part
# must be counted by the output-side substream estimate too (countOutputStreams over the base AND patch parts).
# Before the fix, patch parts were invisible to the estimate entirely. OPTIMIZE reserves memory unconditionally, so
# this is a coverage test - it must still succeed under a pathologically small soft limit, not assert on the
# estimate's numeric value - but it exercises the patch-parts code path added by the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    SET enable_lightweight_update = 1;

    CREATE TABLE t_merge_mem_patch (k UInt64, j JSON)
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1,
        min_bytes_for_wide_part = 0;

    INSERT INTO t_merge_mem_patch SELECT number, ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(3000);

    -- The patch introduces a JSON path ('patch_only') that no base part ever stores.
    UPDATE t_merge_mem_patch SET j = ('{\"a\": ' || toString(k) || ', \"patch_only\": ' || toString(k) || '}')::JSON WHERE k % 2 = 0;

    OPTIMIZE TABLE t_merge_mem_patch FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), countIf(toJSONString(j) LIKE '%patch_only%') FROM t_merge_mem_patch SETTINGS apply_patch_parts = 1;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_patch' AND active AND partition_id = 'all';
" -- --merges_mutations_memory_usage_soft_limit=1
