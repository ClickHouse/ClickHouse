#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# when the source parts are COMPACT and carry semi-structured (JSON) columns with no recorded substreams. A
# compact part written with write_marks_for_substreams_in_compact_parts = 0 records nothing in
# columns_substreams.txt, and - unlike a wide part - it stores every column in a single data.bin, so its dynamic
# substream layout cannot be recovered from on-disk file names. Without a compact-part recovery countOutputStreams
# would collapse such a JSON column to the default one-stream count and undersize the reservation for a merge whose
# result is Wide; the estimate must instead bound the column by its type's write-time capacity. Here the source
# parts stay Compact (fewer rows than min_rows_for_wide_part) while the merged part becomes Wide, so
# countOutputStreams runs over compact sources. Under a pathologically small merges_mutations_memory_usage_soft_limit
# an explicit OPTIMIZE ... FINAL reserves unconditionally, so it must still merge everything down to a single Wide
# part and must not error while estimating the memory of a merge over compact JSON sources.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 keeps the bytes condition from forcing Compact; min_rows_for_wide_part = 2000
    -- then makes each 1000-row insert a Compact part while the 3000-row merged part is Wide.
    -- write_marks_for_substreams_in_compact_parts = 0 makes the compact parts record no substreams, so their JSON
    -- dynamic paths are invisible to columns_substreams.txt and must be recovered by type capacity.
    CREATE TABLE t_merge_mem_compact_json
    (
        k UInt64,
        json JSON
    )
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 2000, write_marks_for_substreams_in_compact_parts = 0;

    SYSTEM STOP MERGES t_merge_mem_compact_json;
    -- Disjoint JSON paths per part so each compact source carries real dynamic substreams that the merged wide
    -- part will materialize.
    INSERT INTO t_merge_mem_compact_json SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_compact_json SELECT number, toJSONString(map('b', number, 'y', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_compact_json SELECT number, toJSONString(map('c', number, 'z', toString(number))) FROM numbers(2000, 1000);

    -- The source parts must be Compact for the compact-recovery path to be exercised.
    SELECT DISTINCT part_type FROM system.parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_compact_json' AND active;

    SYSTEM START MERGES t_merge_mem_compact_json;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_compact_json FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_compact_json;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_compact_json' AND active;
    -- The merged part is Wide, so countOutputStreams (and the compact-source recovery) ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_compact_json' AND active;
    -- The JSON column must still answer queries correctly after the merge (each numeric path lives in only one
    -- source part, so the sums show all three parts' dynamic paths survived into the merged part).
    SELECT sum(json.a.:Int64), sum(json.b.:Int64), sum(json.c.:Int64) FROM t_merge_mem_compact_json;
" -- --merges_mutations_memory_usage_soft_limit=1
