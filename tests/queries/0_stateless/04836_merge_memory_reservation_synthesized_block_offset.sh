#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a rebuilt commit-order projection whose required input is SYNTHESIZED for every merged row.
#
# The rebuilt-projection sizing derives the projected volume from the source parts' own column sizes and adds a
# synthesized-volume term for the rows of the parts that do not store a required column. That term resolved the
# column's type through the metadata's physical columns only, so it silently skipped the persisted
# `_block_number` / `_block_offset` virtuals - which no INSERT ever writes (a merge adds them, see
# MergeTask::addMergingColumn) - and required subcolumns. A projection over `_block_offset` therefore priced
# its rebuild at zero bytes for that column even though the temporary parts hold one value per merged row,
# which can also misclassify them as Compact and shrink both the writer and the read-back reservation.
#
# Here `enable_block_offset_column` is turned on only AFTER the first parts were written, so the merge has to
# synthesize `_block_offset` for all of their rows while rebuilding the projection that reads it. OPTIMIZE
# reserves unconditionally, so this must still succeed under a pathologically small soft limit.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_synth_offset (k UInt64, payload String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0,
             allow_commit_order_projection = 1,
             enable_block_offset_column = 0,
             -- No source part gets the projection materialized, so every part has the same (empty) projection
             -- set and they are mergeable, while the merge still rebuilds the commit-order projection.
             materialize_projections_on_insert = 0,
             materialize_projections_on_merge = 0;

    SYSTEM STOP MERGES t_merge_mem_synth_offset;
    INSERT INTO t_merge_mem_synth_offset SELECT number, repeat('x', 100) FROM numbers(1000);
    INSERT INTO t_merge_mem_synth_offset SELECT number, repeat('x', 100) FROM numbers(1000, 1000);

    -- Only now does the table get the persisted virtual column and the projection reading it, so the parts
    -- above predate both and their _block_offset values are synthesized by the merge.
    ALTER TABLE t_merge_mem_synth_offset MODIFY SETTING enable_block_offset_column = 1;
    ALTER TABLE t_merge_mem_synth_offset ADD PROJECTION p_synth_offset (SELECT _block_offset, payload ORDER BY _block_offset);
    INSERT INTO t_merge_mem_synth_offset SELECT number, repeat('x', 100) FROM numbers(2000, 1000);

    SELECT count() FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_synth_offset' AND active;
    SYSTEM START MERGES t_merge_mem_synth_offset;

    OPTIMIZE TABLE t_merge_mem_synth_offset FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_synth_offset;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_synth_offset' AND active AND partition_id = 'all';
    -- Rebuilt by the merge, which is what the estimator must price: the projection part now exists.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_synth_offset' AND active;
    SELECT count() FROM (SELECT _block_offset FROM t_merge_mem_synth_offset ORDER BY _block_offset);
" -- --merges_mutations_memory_usage_soft_limit=1
