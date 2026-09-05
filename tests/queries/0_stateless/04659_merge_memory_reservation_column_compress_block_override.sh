#!/usr/bin/env bash
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# on a table whose columns override max_compress_block_size at the column level. MergeTreeDataPartWriterWide
# resolves the compressor-block / file-buffer size PER STREAM from the column-level setting (clamped to
# MergeTreeWriterSettings::MAX_COMPRESS_BLOCK_SIZE), so the estimate must size its eager write-buffer term
# from the largest participating override rather than from the table-level setting alone - otherwise a column
# with a larger override allocates bigger eager buffers than were reserved and the admission gate can admit
# more concurrent merges than the reservation bounds. OPTIMIZE reserves unconditionally, so under a
# pathologically small soft limit the merge must still run to a single part and must not error while
# estimating: this drives the override resolution, including a value above the writer's clamp.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "
    SET enable_json_type = 1;

    CREATE TABLE t_merge_mem_column_override
    (
        k UInt64,
        -- Larger than the table-level default (1 MiB): the writer allocates this much per stream.
        big String SETTINGS (max_compress_block_size = 8388608),
        -- Above the writer's clamp (256 MiB): both the writer and the estimate must clamp it.
        huge String SETTINGS (max_compress_block_size = 1073741824),
        -- Smaller than the table-level setting: must not lower the reservation of the other streams.
        small String SETTINGS (max_compress_block_size = 1024),
        -- A semi-structured column mixes overridden and dynamic (adaptive) streams in one estimate.
        j JSON
    )
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_column_override;
    INSERT INTO t_merge_mem_column_override SELECT number, repeat('a', 100), repeat('b', 100), repeat('c', 100), ('{\"a\": ' || toString(number) || '}')::JSON FROM numbers(1000);
    INSERT INTO t_merge_mem_column_override SELECT number, repeat('a', 100), repeat('b', 100), repeat('c', 100), ('{\"b\": ' || toString(number) || '}')::JSON FROM numbers(1000, 1000);
    SYSTEM START MERGES t_merge_mem_column_override;

    OPTIMIZE TABLE t_merge_mem_column_override FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_merge_mem_column_override;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_column_override' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
