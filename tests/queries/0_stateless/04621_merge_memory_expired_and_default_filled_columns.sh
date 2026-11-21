#!/usr/bin/env bash
# Tags: no-fasttest
# Coverage test for the merge memory reservation estimate (CompactionStatistics::estimateNeededMemoryForMerge)
# on two schema-evolution paths of ALTER ... ADD COLUMN over pre-existing parts:
#  - a column added WITHOUT a default is expired for a merge of the old parts (absent from every source part,
#    no default expression): MergeTask erases it from the storage columns and never opens a writer for it, so
#    the estimate must not price its output streams either (an accumulation of late-added semi-structured
#    columns must not saturate merges_mutations_memory_usage_soft_limit for columns that are never written);
#  - a column added WITH a default is kept live and materialized by the merge from the default expression for
#    the rows of the parts that predate the ALTER: its written bytes are synthesized, not read, so the
#    input-volume cap on the writer's data-dependent buffers does not apply to it and its writer streams are
#    priced at their per-stream worst case.
# The reservation amount itself is not observable from SQL, so this exercises the pricing paths
# deterministically: under a pathologically small merges_mutations_memory_usage_soft_limit the explicit
# OPTIMIZE ... FINAL reserves unconditionally and must merge everything down to a single Wide part - with the
# expired column absent from the merged part and the default-filled column materialized correctly - and must
# not error while estimating.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    -- min_bytes_for_wide_part = 0 and min_rows_for_wide_part = 0 force every part (and the merged part) to
    -- be Wide, so the output side is priced per column substream rather than as a compact single stream.

    -- An expired column: added after the parts were written, no default, dynamic structure (JSON), so a
    -- non-filtered estimate would price its write-time capacity streams although the merge never writes it.
    CREATE TABLE t_merge_mem_expired_column (k UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_expired_column;
    INSERT INTO t_merge_mem_expired_column SELECT number FROM numbers(1000);
    INSERT INTO t_merge_mem_expired_column SELECT number FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_expired_column SELECT number FROM numbers(2000, 1000);

    ALTER TABLE t_merge_mem_expired_column ADD COLUMN j JSON;

    -- The premise really holds: no source part stores j.
    SELECT count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_expired_column' AND active AND column = 'j';

    SYSTEM START MERGES t_merge_mem_expired_column;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_merge_mem_expired_column FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), sum(k) FROM t_merge_mem_expired_column;
    SELECT count(), any(part_type) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_expired_column' AND active;
    -- The expired column was not written by the merge: absent from the merged part.
    SELECT count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_expired_column' AND active AND column = 'j';

    -- A default-filled column: the merge reads only k from the old parts but synthesizes and writes s for
    -- every row, so the merged output volume vastly exceeds the bytes read from the source parts.
    CREATE TABLE t_merge_mem_default_filled_column (k UInt64)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_default_filled_column;
    INSERT INTO t_merge_mem_default_filled_column SELECT number FROM numbers(1000);
    INSERT INTO t_merge_mem_default_filled_column SELECT number FROM numbers(1000, 1000);
    INSERT INTO t_merge_mem_default_filled_column SELECT number FROM numbers(2000, 1000);

    ALTER TABLE t_merge_mem_default_filled_column ADD COLUMN s String DEFAULT repeat(toString(k), 100);

    SYSTEM START MERGES t_merge_mem_default_filled_column;

    OPTIMIZE TABLE t_merge_mem_default_filled_column FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), sum(k) FROM t_merge_mem_default_filled_column;
    SELECT count(), any(part_type) FROM system.parts
        WHERE database = currentDatabase() AND table = 't_merge_mem_default_filled_column' AND active;
    -- The default-filled column was materialized by the merge into the merged part, with correct values.
    SELECT count() FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_merge_mem_default_filled_column' AND active AND column = 's';
    SELECT sum(length(s)) FROM t_merge_mem_default_filled_column;
" -- --merges_mutations_memory_usage_soft_limit=1
