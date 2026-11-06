#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the projection REBUILD path when the source parts store the projection's bare-identifier column under
# DIFFERENT types - a supported upgrade path (03918_json_lazy_type_hints_merge): a JSON part written before
# ALTER TABLE ... MODIFY COLUMN j JSON(val UInt32) merged with a newer hinted part. When the merge rebuilds a
# SELECT json ... projection, writeTempProjectionPart reserializes rows from BOTH parts under the current
# metadata, so the rebuilt column carries the old-part-only dynamic paths too. tryCountBareIdentifierProjectionSubstreams
# unions recorded substreams only over parts of the SAME type as the projection output, so pricing the projection
# from that union alone would drop the different-type parts' dynamic paths; the estimate must instead fall back to
# the projection column type's write-time capacity, which no rebuilt column can exceed. Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, so it must still
# merge everything down to a single part with the rebuilt projection intact, and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- Lazy JSON type hints keep ALTER MODIFY COLUMN metadata-only, so existing parts keep their plain JSON on
    -- disk (converted only on merge) - exactly the mixed old/new part scenario the estimate must handle.
    SET allow_experimental_json_lazy_type_hints = 1;
    SET allow_suspicious_types_in_order_by = 1;

    -- min_bytes_for_wide_part = 0 forces the Wide format so the per-substream estimate path is exercised, and
    -- materialize_projections_on_merge = 1 makes the merge rebuild a projection that no source part has.
    CREATE TABLE t_merge_mem_mixed_json
    (
        k UInt64,
        json JSON
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, materialize_projections_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_mixed_json;
    -- Two parts written while json is a plain JSON (no type hint), so their on-disk type differs from the hinted
    -- type below and from the projection output type.
    INSERT INTO t_merge_mem_mixed_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_mixed_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(1000, 1000);

    -- Add a type hint (lazy, metadata only): existing parts keep their plain JSON on disk, new parts use the hint.
    ALTER TABLE t_merge_mem_mixed_json MODIFY COLUMN json JSON(a UInt64);

    -- A part written with the hinted type, so the merge mixes JSON and JSON(a UInt64) source parts.
    INSERT INTO t_merge_mem_mixed_json SELECT number, toJSONString(map('a', number, 'b', number * 2, 'c', toString(number))) FROM numbers(2000, 1000);

    -- The source parts really do store json under different types (the two plain-JSON parts and the hinted one).
    SELECT name, type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_json' AND active AND column = 'json'
        ORDER BY name;

    -- Add the projection AFTER the parts exist, so none of them has it materialized. The merge below rebuilds it
    -- from the merged rows (the materialize_projections_on_merge rebuild branch). json is a bare identifier here,
    -- so the estimate tries to price it from the source parts' recorded substreams - which only cover the hinted
    -- part; the different-type plain-JSON parts must force the type-capacity fallback.
    ALTER TABLE t_merge_mem_mixed_json ADD PROJECTION p_json (SELECT json ORDER BY json.a);

    SYSTEM START MERGES t_merge_mem_mixed_json;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_mixed_json FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_mixed_json;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_json' AND active;
    -- The rebuilt projection must be present in the single merged part.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_json' AND active
        ORDER BY name;
    -- And it must still answer queries correctly after the merge (the typed path survived from all three parts).
    SELECT sum(json.a) FROM t_merge_mem_mixed_json;
" -- --merges_mutations_memory_usage_soft_limit=1
