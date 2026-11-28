#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path with a semi-structured (JSON) projection column. When some source parts lack a
# projection and materialize_projections_on_merge is set, the merge rebuilds the projection from the merged base
# rows (MergeTask::ExecuteAndFinalizeHorizontalPart::prepareProjectionsToMergeAndRebuild), and a rebuilt JSON
# projection column writes one on-disk stream per dynamic path - the same layout as the base column it is derived
# from. The estimate must price it against the source parts by name (countColumnStreamsFromParts) rather than the
# default serialization, which would collapse the JSON column to a single stream and undersize the reservation.
# Under a pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything down to a single part with the rebuilt projection intact, and
# must not error while estimating the memory of a merge that also rebuilds a JSON projection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised, and
    -- materialize_projections_on_merge = 1 makes the merge rebuild a projection that some parts do not have.
    -- json.a is a typed path (usable as the projection sort key); b and c are dynamic paths, so the base json
    -- column carries several real dynamic substreams for the rebuilt projection to be priced against.
    CREATE TABLE t_merge_mem_rebuilt_json
    (
        k UInt64,
        json JSON(a UInt64)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_rebuilt_json;
    -- Several distinct JSON paths per part so the base column has real dynamic substreams to price against.
    INSERT INTO t_merge_mem_rebuilt_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_rebuilt_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_rebuilt_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(2000, 1000);

    -- Add the projection AFTER the parts exist, so none of them has it materialized. The merge below rebuilds
    -- it from the merged rows (the materialize_projections_on_merge rebuild branch), not by merging existing
    -- projection parts.
    ALTER TABLE t_merge_mem_rebuilt_json ADD PROJECTION p_json (SELECT json ORDER BY json.a);

    SYSTEM START MERGES t_merge_mem_rebuilt_json;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_rebuilt_json FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_rebuilt_json;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_json' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_json' AND active
        ORDER BY name;
    -- And it must still answer queries correctly after the merge.
    SELECT sum(json.a) FROM t_merge_mem_rebuilt_json;
" -- --merges_mutations_memory_usage_soft_limit=1
