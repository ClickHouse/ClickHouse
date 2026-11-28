#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection-part column-drift rebuild trigger. Projection metadata is re-derived from the projection query
# at every table load, so an existing projection part may lack a column the current metadata expects (here: an
# ALIAS column selected by the projection is re-pointed by ALTER). MergeTask::prepareProjectionsToMergeAndRebuild
# then REBUILDS the projection from the merged parent rows instead of merging the drifted projection parts, and
# the reservation estimate must price the rebuild (temp-part writers + read-back, including the streams of the
# newly expected column) rather than a merge of the stale projection parts. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, must merge
# everything down to a single part while rebuilding the drifted projection, must not error while estimating, and
# the rebuilt projection must serve the repointed alias.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_merge_mem_proj_drift
    (
        a UInt64,
        b UInt64,
        d UInt64,
        c UInt64 ALIAS b + 1,
        PROJECTION p (SELECT a, c ORDER BY a)
    )
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_proj_drift;
    INSERT INTO t_merge_mem_proj_drift (a, b, d) SELECT number, number * 10, number * 100 FROM numbers(1000);
    INSERT INTO t_merge_mem_proj_drift (a, b, d) SELECT number, number * 10, number * 100 FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_proj_drift (a, b, d) SELECT number, number * 10, number * 100 FROM numbers(2000, 1000);

    -- Re-point the alias: the projection parts still store the old alias source, but the re-derived
    -- projection metadata now expects the new one - every existing projection part is drifted, so the
    -- merge below takes the projection_part_misses_column REBUILD path the estimate must mirror.
    ALTER TABLE t_merge_mem_proj_drift MODIFY COLUMN c UInt64 ALIAS d + 1;

    SYSTEM START MERGES t_merge_mem_proj_drift;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_proj_drift FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_proj_drift;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_proj_drift' AND active;
    -- The rebuilt projection must serve the repointed alias (d + 1), not stale or default-filled values.
    SELECT sum(c) FROM t_merge_mem_proj_drift SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
" -- --merges_mutations_memory_usage_soft_limit=1
