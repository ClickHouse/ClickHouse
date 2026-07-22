#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on the persisted _block_number / _block_offset columns. When enable_block_number_column /
# enable_block_offset_column are on, MergeTask writes these two virtual columns on top of the metadata's physical
# columns (addMergingColumn / addGatheringColumn), so the estimate must price their writer streams too. When the
# settings were enabled AFTER the source parts were written (this test), no source part stores them: the merge
# readers synthesize their values from each part's own block number, so - like a column filled from its DEFAULT
# expression - their written bytes are not bounded by the bytes the merge reads and they are priced from the
# synthesized volume (rows times the fixed value size). Under a pathologically small
# merges_mutations_memory_usage_soft_limit an explicit OPTIMIZE ... FINAL reserves unconditionally, must merge
# everything down to a single part, must not error while estimating, and the merged part must physically store
# both columns (the very streams now priced).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_merge_mem_block_cols (k UInt64, v String)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_block_cols;
    INSERT INTO t_merge_mem_block_cols SELECT number, toString(number) FROM numbers(1000);
    INSERT INTO t_merge_mem_block_cols SELECT number, toString(number) FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_block_cols SELECT number, toString(number) FROM numbers(2000, 1000);

    -- Enable the persisted block columns (and the extended part min-max index over them) only after the
    -- source parts were written: they are absent from every source part, so the merge synthesizes and
    -- writes them - the upgrade path the estimate must price.
    ALTER TABLE t_merge_mem_block_cols MODIFY SETTING
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        part_minmax_index_columns = 'with_block_number_offset';

    SYSTEM START MERGES t_merge_mem_block_cols;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_block_cols FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_block_cols;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_block_cols' AND active;
    -- The merged part physically stores the persisted block columns whose writer streams the estimate priced.
    SELECT column FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_block_cols' AND active AND column LIKE '\_block%'
        ORDER BY column;
    SELECT sum(k) FROM t_merge_mem_block_cols;
" -- --merges_mutations_memory_usage_soft_limit=1
