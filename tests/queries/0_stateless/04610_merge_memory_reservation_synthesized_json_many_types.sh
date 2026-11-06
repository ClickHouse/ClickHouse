#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# of a rebuilt projection whose SYNTHESIZED semi-structured columns concentrate many concrete types into few
# dynamic slots: a `JSON(max_dynamic_paths=1, max_dynamic_types=...)` whose single dynamic path sees many
# different concrete types across the merged rows, and a `Dynamic` synthesized from a composite value. Such a
# path is routed through SerializationDynamic -> SerializationVariant and writes one stream group per concrete
# variant, so the estimate must price each dynamic path by the full Dynamic capacity of its max_dynamic_types
# (see countDynamicCapacityStreams), not a couple of streams per path. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE reserves unconditionally, so it must still
# merge everything down to a single part with the projection materialized, and must not error while estimating
# the memory of the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_synth_json_many (k UInt64, i Int64, s String, f Float64, a Array(UInt64))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_synth_json_many;
    INSERT INTO t_merge_mem_synth_json_many SELECT number, number, toString(number), number / 3, range(number % 4) FROM numbers(1000);
    INSERT INTO t_merge_mem_synth_json_many SELECT number, -number, toString(number), number / 7, range(number % 4) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_synth_json_many SELECT number, number, toString(number), number / 9, range(number % 4) FROM numbers(2000, 1000);

    -- Added after the parts are written, so no source part has it materialized; with
    -- materialize_projections_on_merge the merge takes the rebuild path for it. The single dynamic path of
    -- the JSON('max_dynamic_paths=1') column receives Int64 / String / Float64 / Array(UInt64) across the
    -- rows, and the Dynamic column is synthesized from a composite tuple, so both stress the per-variant
    -- capacity term of the estimate.
    ALTER TABLE t_merge_mem_synth_json_many ADD PROJECTION p_many (
        SELECT
            k,
            CAST(map('p', if(k % 4 = 0, toJSONString(i), if(k % 4 = 1, s, if(k % 4 = 2, toJSONString(f), toJSONString(a))))), 'JSON(max_dynamic_paths=1)') AS j,
            CAST(tuple(i, a, s) AS Dynamic) AS d
        ORDER BY k);
    SYSTEM START MERGES t_merge_mem_synth_json_many;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_synth_json_many FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_synth_json_many;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_synth_json_many' AND active;
    -- The projection must have been rebuilt into the merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_synth_json_many' AND active
        ORDER BY name;
    -- And the data must be correct after the merge.
    SELECT sum(k) FROM t_merge_mem_synth_json_many;
" -- --merges_mutations_memory_usage_soft_limit=1
