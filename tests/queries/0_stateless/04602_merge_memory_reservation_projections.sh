#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on tables with projections. A merge also reads and writes projection parts: fully-present projections are
# merged by a nested MergeTask over the source parts' projection parts, so the estimate prices that nested
# merge recursively. Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit
# OPTIMIZE ... FINAL reserves unconditionally, so it must still merge everything down to a single part with
# all projections intact, and must not error while estimating the memory of a merge that also merges
# projection parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised
    -- for the base parts and for the projection parts alike.
    CREATE TABLE t_merge_mem_projections
    (
        k UInt64,
        v UInt64,
        s String,
        PROJECTION p_order (SELECT s, v ORDER BY v),
        PROJECTION p_agg (SELECT k % 10, sum(v) GROUP BY k % 10)
    )
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_projections;
    INSERT INTO t_merge_mem_projections SELECT number, number * 2, toString(number) FROM numbers(1000);
    INSERT INTO t_merge_mem_projections SELECT number, number * 2, toString(number) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_projections SELECT number, number * 2, toString(number) FROM numbers(2000, 1000);
    SYSTEM START MERGES t_merge_mem_projections;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_projections FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_projections;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_projections' AND active;
    -- Both projections must survive the merge in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_projections' AND active
        ORDER BY name;
    -- And they must still answer queries correctly after the merge.
    SELECT sum(v) FROM (SELECT sum(v) AS v FROM t_merge_mem_projections GROUP BY k % 10);
" -- --merges_mutations_memory_usage_soft_limit=1
