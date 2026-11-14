#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# of a rebuilt projection that SYNTHESIZES semi-structured values from ordinary columns: `CAST(v, 'Dynamic')`
# and a `JSON` parsed from a `String`. No source part records substreams for such expression-produced columns
# and their dynamic streams do not derive from any semi-structured input, so the estimate prices them by the
# type's own write-time capacity (see countDynamicCapacityStreams) instead of a source-derived bound. Under a
# pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE reserves unconditionally,
# so it must still merge everything down to a single part with the projection materialized, and must not error
# while estimating the memory of the rebuild.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised.
    CREATE TABLE t_merge_mem_synth_dynamic (k UInt64, v UInt64, s String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_synth_dynamic;
    INSERT INTO t_merge_mem_synth_dynamic SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(1000);
    INSERT INTO t_merge_mem_synth_dynamic SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_synth_dynamic SELECT number, number * 2, toJSONString(map('a', number)) FROM numbers(2000, 1000);

    -- Added after the parts are written, so no source part has it materialized; with
    -- materialize_projections_on_merge the merge takes the rebuild path for it.
    ALTER TABLE t_merge_mem_synth_dynamic ADD PROJECTION p_synth (SELECT k, CAST(v, 'Dynamic') AS d, CAST(s, 'JSON') AS j ORDER BY k);
    SYSTEM START MERGES t_merge_mem_synth_dynamic;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_synth_dynamic FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_synth_dynamic;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_synth_dynamic' AND active;
    -- The projection must have been rebuilt into the merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_synth_dynamic' AND active
        ORDER BY name;
    -- And the data must be correct after the merge.
    SELECT sum(v) FROM t_merge_mem_synth_dynamic;
" -- --merges_mutations_memory_usage_soft_limit=1
