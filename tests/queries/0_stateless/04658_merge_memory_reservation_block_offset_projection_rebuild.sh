#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a commit-order projection that reads `_block_offset` when NO source part has the projection materialized.
# MergeTask::prepareProjectionsToMergeAndRebuild rebuilds such a projection from the merged rows - since
# d673d9e5a6e ("Introduce Invalidated System Columns") a `_block_offset` projection takes that branch together
# with a `_block_number` one, because a merge invalidates `_block_offset` and the projection cannot be merged from
# stale per-part offsets. The estimator only mirrored `with_block_number`, so a `_block_offset`-only projection took
# the drop branch and its whole rebuild - the temporary projection part writer streams plus the read-back - was
# missing from the reservation, under-reserving exactly the path the merge executes. OPTIMIZE reserves
# unconditionally, so this must still succeed under a pathologically small soft limit while driving the rebuilt
# projection sizing for a commit-order projection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_block_offset_proj
    (
        k UInt64,
        payload String,
        PROJECTION p_offset (SELECT _block_offset, payload ORDER BY _block_offset)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0,
             allow_commit_order_projection = 1,
             enable_block_offset_column = 1,
             -- No source part gets the projection materialized, so every part has the same (empty) projection set
             -- and they are mergeable, while the merge still sees 'some parts do not have it' and rebuilds.
             materialize_projections_on_insert = 0,
             -- Off, so the rebuild is decided by the projection being a commit-order one, not by this setting.
             materialize_projections_on_merge = 0;

    SYSTEM STOP MERGES t_merge_mem_block_offset_proj;
    INSERT INTO t_merge_mem_block_offset_proj SELECT number, repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_merge_mem_block_offset_proj SELECT number, repeat('x', 100) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_block_offset_proj SELECT number, repeat('x', 100) FROM numbers(2000, 1000);

    -- The projection is not materialized in any source part.
    SELECT count() FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_block_offset_proj' AND active;
    SYSTEM START MERGES t_merge_mem_block_offset_proj;

    OPTIMIZE TABLE t_merge_mem_block_offset_proj FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_block_offset_proj;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_block_offset_proj' AND active AND partition_id = 'all';
    -- Rebuilt by the merge, which is what the estimator must price: the projection part now exists.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_block_offset_proj' AND active;
    SELECT count() FROM (SELECT _block_offset FROM t_merge_mem_block_offset_proj ORDER BY _block_offset);
" -- --merges_mutations_memory_usage_soft_limit=1
