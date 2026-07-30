#!/usr/bin/env bash
# Regression test pinning what MergeTask::prepareProjectionsToMergeAndRebuild does with a projection that reads
# `_block_offset` (and not `_block_number`) when NO source part has the projection materialized.
# The merge memory reservation estimate (CompactionStatistics::estimateNeededMemoryForMerge) mirrors that decision:
# with some source parts missing the projection, the merge rebuilds it only for a `_block_number` (commit-order)
# projection - which is never written on insert - or under materialize_projections_on_merge, and DROPS it from the
# merged part otherwise. A `_block_offset` projection is written on insert like any ordinary one, so it must take
# the drop branch here and cost the reservation nothing. Pricing a rebuild the merge never performs would
# over-reserve, the starvation direction this estimate exists to avoid. This test pins the merge behaviour the
# estimator mirrors: the projection is absent from the merged part.

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
             -- and they are mergeable, while the merge still sees 'some parts do not have it'.
             materialize_projections_on_insert = 0,
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
    -- Dropped by the merge, exactly as the estimator prices it: still no projection part.
    SELECT count() FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_block_offset_proj' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
