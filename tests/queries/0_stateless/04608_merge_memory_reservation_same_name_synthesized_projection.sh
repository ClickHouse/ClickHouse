#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# of a rebuilt projection whose output columns REUSE a base column's NAME for a different type - a synthesized
# semi-structured value, not a bare identifier: `CAST(v, 'Dynamic') AS v` over a UInt64 base `v`, and
# `CAST(s, 'JSON(max_dynamic_paths=0)') AS s` over a String base `s`. Pricing such a column from the base
# column's recorded substreams (as a same-name projection) would charge the UInt64 / String stream count
# instead of the rebuilt semi-structured column's write footprint (countRebuiltProjectionStreams now takes the
# precise by-name branch only when the base column has the SAME type, otherwise bounds by the type's write-time
# capacity via countDynamicCapacityStreams). A `JSON(max_dynamic_paths=0)` layout also stores every path in the
# shared data, whose streams the estimate must still account for. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE reserves unconditionally, so it must still merge
# everything down to a single part with the projection materialized, and must not error while estimating the
# memory of the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_same_name (k UInt64, v UInt64, s String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_same_name;
    INSERT INTO t_merge_mem_same_name SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(1000);
    INSERT INTO t_merge_mem_same_name SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_same_name SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(2000, 1000);

    -- Added after the parts are written, so no source part has it materialized; with
    -- materialize_projections_on_merge the merge takes the rebuild path for it. The projection output columns
    -- 'v' and 's' reuse the base column names for a Dynamic / JSON type respectively - a same-name but
    -- different-type synthesized column, not a bare identifier.
    ALTER TABLE t_merge_mem_same_name ADD PROJECTION p_same_name
        (SELECT k, CAST(v, 'Dynamic') AS v, CAST(s, 'JSON(max_dynamic_paths=0)') AS s ORDER BY k);
    SYSTEM START MERGES t_merge_mem_same_name;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_same_name FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_same_name;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_same_name' AND active;
    -- The projection must have been rebuilt into the merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_same_name' AND active
        ORDER BY name;
    -- And the data must be correct after the merge.
    SELECT sum(v) FROM t_merge_mem_same_name;
" -- --merges_mutations_memory_usage_soft_limit=1
