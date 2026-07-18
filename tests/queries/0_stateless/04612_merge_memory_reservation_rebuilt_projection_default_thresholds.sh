#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path with the DEFAULT wide-part thresholds. writeTempProjectionPart chooses the
# temporary projection part's format with choosePartFormat, so with the default min_bytes_for_wide_part (10 MiB) a
# small rebuilt projection block is written as a Compact part: one shared writer buffer, and MergeProjectionPartsTask
# reads it back through one shared reader buffer per part - not one buffer per substream. The estimate must mirror
# that decision and price a Compact temporary projection part as a single stream, otherwise a semi-structured (JSON)
# projection would be reserved as if it wrote one buffer per dynamic substream, over-reserving by orders of magnitude
# and serializing background merges. Unlike the rest of this family, this test deliberately keeps the default
# min_bytes_for_wide_part so the Compact temporary-projection-part path is exercised. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, so it must still
# merge everything down to a single part with the rebuilt projection intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- Default min_bytes_for_wide_part (10 MiB): the small rebuilt projection block is written as a Compact
    -- temporary part, so the estimate must price it as a single stream rather than one per JSON substream.
    -- materialize_projections_on_merge = 1 makes the merge rebuild a projection that no source part has.
    -- json.a is a typed path (usable as the projection sort key); b and c are dynamic paths, so the rebuilt
    -- JSON projection column carries several real dynamic substreams.
    CREATE TABLE t_merge_mem_rebuilt_json_default
    (
        k UInt64,
        json JSON(a UInt64)
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_rebuilt_json_default;
    INSERT INTO t_merge_mem_rebuilt_json_default SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_rebuilt_json_default SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_rebuilt_json_default SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(2000, 1000);

    -- Add the projection AFTER the parts exist, so none of them has it materialized. The merge below rebuilds it
    -- from the merged rows (the materialize_projections_on_merge rebuild branch).
    ALTER TABLE t_merge_mem_rebuilt_json_default ADD PROJECTION p_json (SELECT json ORDER BY json.a);

    SYSTEM START MERGES t_merge_mem_rebuilt_json_default;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_rebuilt_json_default FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_rebuilt_json_default;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_json_default' AND active;
    -- The temporary projection part (and the merged part) are Compact under the default threshold.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_json_default' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_rebuilt_json_default' AND active
        ORDER BY name;
    -- And it must still answer queries correctly after the merge.
    SELECT sum(json.a) FROM t_merge_mem_rebuilt_json_default;
" -- --merges_mutations_memory_usage_soft_limit=1
