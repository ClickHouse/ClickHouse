#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge
# / partReadBytes / countPartStreamsForColumns) on the input side: the estimate must charge only the columns the
# base merge actually reads, not the whole source part. Two ways a whole-part estimate over-reserves:
#  - after a metadata-only ALTER ... DROP COLUMN (the mutation has not materialized yet), the old wide parts still
#    physically store the dropped semi-structured column - its .bin files, columns.txt entry and bytes - but the
#    merge never opens a reader for it and never writes it;
#  - a parent part's bytes_on_disk includes its projection parts (.proj), whose IO the estimator prices separately,
#    so charging them again in the base input cap double-counts the projection.
# The reservation amount itself is not observable from SQL, so this exercises the pricing paths deterministically:
# under a pathologically small merges_mutations_memory_usage_soft_limit the explicit OPTIMIZE ... FINAL reserves
# unconditionally and must merge everything down to a single Wide part - with the dead column absent from the
# merged part and the projection carried over - and must not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force every part (and the merged part) to be
    -- Wide, so the dropped column really leaves per-column .bin files behind and the per-substream input
    -- pricing is exercised rather than the compact single-stream one. The projection is declared at CREATE
    -- time so every part materializes it (parts with different projection sets cannot be merged) and the
    -- merge prices the nested projection merge separately from the base input.
    CREATE TABLE t_merge_mem_dropped_column
    (
        k UInt64,
        d JSON,
        PROJECTION p (SELECT k ORDER BY k)
    )
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_dropped_column;
    -- Three wide parts that physically store d with real dynamic paths (and each a projection part).
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000);
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_dropped_column SELECT number, toJSONString(map('a', number, 'x', toString(number))) FROM numbers(2000, 1000);

    -- Metadata-only for now: alter_sync = 0 returns without waiting for the mutation that would rewrite
    -- the parts, and under the tiny soft limit below that mutation is never scheduled, so the old parts
    -- keep the dead column's files on disk.
    ALTER TABLE t_merge_mem_dropped_column DROP COLUMN d SETTINGS alter_sync = 0;

    -- The premise really holds: every source part still physically stores d after the metadata drop.
    SELECT name, part_type FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active AND column = 'd'
        ORDER BY name;

    SYSTEM START MERGES t_merge_mem_dropped_column;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_dropped_column FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_dropped_column;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active;
    -- The merged part is Wide, so the per-substream input pricing ran during selection.
    SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active;
    -- The merge wrote only the current metadata's columns: the dead column is gone from the merged part.
    SELECT count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active AND column = 'd';
    -- The projection survived the merge of the projection parts.
    SELECT name FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_dropped_column' AND active;
    SELECT sum(k) FROM t_merge_mem_dropped_column;
" -- --merges_mutations_memory_usage_soft_limit=1
