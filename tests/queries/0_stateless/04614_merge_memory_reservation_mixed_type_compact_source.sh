#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the countOutputStreams COMPACT-source recovery path when the compact source parts store a JSON column under
# DIFFERENT types - a supported upgrade path (03918_json_lazy_type_hints_merge): a plain JSON part written before
# ALTER TABLE ... MODIFY COLUMN j JSON(val UInt32) merged with newer hinted parts. A compact part written with
# write_marks_for_substreams_in_compact_parts = 0 stores every column in a single data.bin, so it records no
# substreams and its dynamic layout cannot be recovered from disk; countOutputStreams bounds each of its
# dynamic-structure columns by the type's write-time capacity instead. That capacity must be taken from the OUTPUT
# (merged, current-metadata) column type - JSON(a UInt64) here - not the first source part's own (plain JSON) type,
# because the merged Wide part reserializes the column under the current metadata; pricing the older, narrower
# source type would undersize the reservation. Under a pathologically small merges_mutations_memory_usage_soft_limit
# an explicit OPTIMIZE ... FINAL reserves unconditionally, so it must still merge everything down to a single Wide
# part and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- Lazy JSON type hints keep ALTER MODIFY COLUMN metadata-only, so the first part keeps its plain JSON on disk
    -- (converted only on merge) while newer parts use the hinted type - a mixed old/new compact merge.
    SET allow_experimental_json_lazy_type_hints = 1;

    -- min_bytes_for_wide_part = 0 keeps the bytes condition from forcing Compact; min_rows_for_wide_part = 2000
    -- then makes each 1000-row insert a Compact part while the 3000-row merged part is Wide.
    -- write_marks_for_substreams_in_compact_parts = 0 makes the compact parts record no substreams, so their JSON
    -- dynamic paths are invisible to columns_substreams.txt and must be recovered by type capacity.
    CREATE TABLE t_merge_mem_mixed_compact
    (
        k UInt64,
        json JSON
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 2000, write_marks_for_substreams_in_compact_parts = 0;

    SYSTEM STOP MERGES t_merge_mem_mixed_compact;
    -- First compact part written while json is a plain JSON (no type hint).
    INSERT INTO t_merge_mem_mixed_compact SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000);

    -- Add a type hint (lazy, metadata only): the first part keeps plain JSON on disk, new parts use the hint.
    ALTER TABLE t_merge_mem_mixed_compact MODIFY COLUMN json JSON(a UInt64);

    -- Two more compact parts written with the hinted type, so the merge mixes JSON and JSON(a UInt64) compact sources.
    INSERT INTO t_merge_mem_mixed_compact SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_mixed_compact SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(2000, 1000);

    -- The source parts really do store json under different types, and are all Compact, so the compact-source
    -- recovery over a type-mismatched part is exercised.
    SELECT name, type, part_type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_compact' AND active AND column = 'json'
        ORDER BY name;

    SYSTEM START MERGES t_merge_mem_mixed_compact;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_mixed_compact FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_mixed_compact;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_compact' AND active;
    -- The merged part is Wide, so countOutputStreams (and the compact-source recovery) ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_compact' AND active;
    -- The JSON column must still answer queries correctly after the merge (the typed path survived from all parts).
    SELECT sum(json.a) FROM t_merge_mem_mixed_compact;
" -- --merges_mutations_memory_usage_soft_limit=1
