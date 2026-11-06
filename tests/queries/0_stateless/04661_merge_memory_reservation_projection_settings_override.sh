#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a table whose PROJECTION overrides MergeTree writer settings with WITH SETTINGS. A projection is written
# through getSettings(&projection.settings_changes) (see writeProjectionPartImpl), so a projection that raises
# max_compress_block_size allocates bigger eager write buffers per stream than the parent table's setting
# describes - the estimate must price both projection paths with the projection's own effective settings,
# otherwise the admission gate can admit more concurrent merges than the reservation bounds.
#
# Both projection paths are driven:
#  - t_merge_mem_projection_settings_merged: every source part has the projection, so the merge merges the
#    projection parts with a nested MergeTask, which the estimate prices by recursing with the projection's
#    metadata - and now with the projection's settings;
#  - t_merge_mem_projection_settings_rebuilt: the parts predate the projection and
#    materialize_projections_on_merge rebuilds it from the merged rows, which the estimate prices as a
#    temp-part writer (its per-stream buffers and remnants sized from the projection's settings) plus the
#    read-back of the temporary parts.
# OPTIMIZE reserves unconditionally, so under a pathologically small soft limit both merges must still run to
# a single part with the projection present, and must not error while estimating. That makes this an
# end-to-end coverage test of the two projection paths - it exercises the estimator on them and checks the
# merge result - and not a check of the reserved amount itself: the reservation the estimator returns is
# asserted in 05023_merge_memory_reservation_projection_settings_reserved, which holds a background merge on
# a failpoint and compares the reservation of a projection that raises max_compress_block_size against one
# that inherits the table's.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_projection_settings_merged
    (
        k UInt64,
        v String,
        -- Raised well above the table-level default (1 MiB), and one value above the writer's clamp.
        PROJECTION p_big (SELECT k, v ORDER BY v) WITH SETTINGS (max_compress_block_size = 8388608),
        PROJECTION p_huge (SELECT k, v ORDER BY k, v) WITH SETTINGS (max_compress_block_size = 1073741824)
    )
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = 1048576;

    SYSTEM STOP MERGES t_merge_mem_projection_settings_merged;
    INSERT INTO t_merge_mem_projection_settings_merged SELECT number, repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_merge_mem_projection_settings_merged SELECT number, repeat('b', 100) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_projection_settings_merged SELECT number, repeat('c', 100) FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_projection_settings_merged;

    OPTIMIZE TABLE t_merge_mem_projection_settings_merged FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_projection_settings_merged;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_projection_settings_merged' AND active;
    SELECT name FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_mem_projection_settings_merged' AND active ORDER BY name;
" -- --merges_mutations_memory_usage_soft_limit=1

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_projection_settings_rebuilt
    (
        k UInt64,
        v String
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, max_compress_block_size = 1048576, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_projection_settings_rebuilt;
    INSERT INTO t_merge_mem_projection_settings_rebuilt SELECT number, repeat('a', 100) FROM numbers(1000);
    INSERT INTO t_merge_mem_projection_settings_rebuilt SELECT number, repeat('b', 100) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_projection_settings_rebuilt SELECT number, repeat('c', 100) FROM numbers(2000, 1000);

    -- Added after the inserts, so no source part has it and the merge has to rebuild it from the merged rows.
    ALTER TABLE t_merge_mem_projection_settings_rebuilt
        ADD PROJECTION p_rebuilt (SELECT k, v ORDER BY v) WITH SETTINGS (max_compress_block_size = 8388608);

    SYSTEM START MERGES t_merge_mem_projection_settings_rebuilt;

    OPTIMIZE TABLE t_merge_mem_projection_settings_rebuilt FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_projection_settings_rebuilt;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_projection_settings_rebuilt' AND active;
    SELECT name FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_merge_mem_projection_settings_rebuilt' AND active ORDER BY name;
" -- --merges_mutations_memory_usage_soft_limit=1
