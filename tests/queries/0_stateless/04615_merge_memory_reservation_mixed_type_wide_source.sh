#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / countOutputStreams) when WIDE source parts store a JSON column under DIFFERENT types - a supported metadata-only
# upgrade path (03918_json_lazy_type_hints_merge): a plain JSON wide part written before ALTER TABLE ... MODIFY
# COLUMN j JSON(a UInt64) merged with newer hinted wide parts. A wide part keeps its columns_substreams.txt for its
# own, narrower type, so the per-column union of recorded substreams (matched by name) undercounts the streams the
# merged part writes when it reserializes the old rows under the current, wider type. countOutputStreams must detect
# the type mismatch and bound such a column by the OUTPUT (merged, current-metadata) type's write-time capacity
# instead, which no merged column can exceed - the same treatment the compact-source recovery already applies. Under
# a pathologically small merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves
# unconditionally, so it must still merge everything down to a single Wide part and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- Lazy JSON type hints keep ALTER MODIFY COLUMN metadata-only, so the first part keeps its plain JSON on disk
    -- (converted only on merge) while newer parts use the hinted type - a mixed old/new wide merge.
    SET allow_experimental_json_lazy_type_hints = 1;

    -- min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force every part (and the merged part) to be Wide,
    -- so the countOutputStreams per-substream / type-mismatch path is exercised rather than the compact one.
    CREATE TABLE t_merge_mem_mixed_wide
    (
        k UInt64,
        json JSON
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_mixed_wide;
    -- First wide part written while json is a plain JSON (no type hint).
    INSERT INTO t_merge_mem_mixed_wide SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000);

    -- Add a type hint (lazy, metadata only): the first part keeps plain JSON on disk, new parts use the hint.
    ALTER TABLE t_merge_mem_mixed_wide MODIFY COLUMN json JSON(a UInt64);

    -- Two more wide parts written with the hinted type, so the merge mixes JSON and JSON(a UInt64) wide sources.
    INSERT INTO t_merge_mem_mixed_wide SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_mixed_wide SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(2000, 1000);

    -- The source parts really do store json under different types, and are all Wide, so the type-mismatch bailout
    -- on the wide output path is exercised.
    SELECT name, type, part_type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_wide' AND active AND column = 'json'
        ORDER BY name;

    SYSTEM START MERGES t_merge_mem_mixed_wide;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_mixed_wide FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_mixed_wide;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_wide' AND active;
    -- The merged part is Wide, so countOutputStreams (and the type-mismatch bailout) ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_mixed_wide' AND active;
    -- The JSON column must still answer queries correctly after the merge (the typed path survived from all parts).
    SELECT sum(json.a) FROM t_merge_mem_mixed_wide;
" -- --merges_mutations_memory_usage_soft_limit=1
