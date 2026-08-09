#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the JSON type is not available in the fast test.
#
# Coverage test for the merge memory reservation estimate (see CompactionStatistics::estimateNeededMemoryForMerge)
# when a column WITHOUT an explicit DEFAULT is stored by some source parts and missing from others. The merge
# keeps such a column alive as soon as any base part or patch part stores it, and IMergeTreeReader's
# fillMissingColumns materializes the type's default values for the rows of the parts that lack it - an explicit
# DEFAULT expression is not required. The estimate used to treat only explicit-DEFAULT columns as synthesized
# output, so the written bytes of an implicitly-filled column were assumed to be covered by the input-volume
# bound (they are not: the missing rows' values are never read), and an implicitly-filled JSON / Dynamic column
# kept the recorded-substream union treated as exact. It now treats ANY output column missing from some base part
# as default-filled.
#
# OPTIMIZE reserves unconditionally, so under a pathologically small soft limit the merges must still run to a
# single part with correct type-default values for the older rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Phase 1: a plain ALTER ... ADD COLUMN (no DEFAULT) of a scalar and of a JSON column, followed by inserts that
# store them - the columns are missing from the pre-ALTER part and present in the post-ALTER parts.
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_implicit_default (k UInt64, payload String)
    ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_merge_mem_implicit_default;
    -- One part written BEFORE the ALTER: it does not store d or j at all.
    INSERT INTO t_merge_mem_implicit_default SELECT number, repeat('x', 100) FROM numbers(1000);

    -- Metadata-only ALTER with NO DEFAULT: the missing rows get the types' own default values.
    ALTER TABLE t_merge_mem_implicit_default ADD COLUMN d UInt64, ADD COLUMN j JSON;

    -- Parts written AFTER the ALTER physically store both columns, keeping them alive for the merge.
    INSERT INTO t_merge_mem_implicit_default
        SELECT number, repeat('x', 100), number, toJSONString(map('a', repeat('y', 50), 'n', toString(number)))
        FROM numbers(1000, 1000);

    SYSTEM START MERGES t_merge_mem_implicit_default;
    OPTIMIZE TABLE t_merge_mem_implicit_default FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count(), countIf(d = 0), countIf(j.a IS NULL) FROM t_merge_mem_implicit_default;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_implicit_default' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1

# Phase 2: the column's only live values sit in a patch part - ADD COLUMN (no DEFAULT) followed by a lightweight
# UPDATE into it, with no base part storing the column at all. The merge keeps it alive through the patch and
# fills every base row with the type default before the patch applies.
${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE t_merge_mem_implicit_default_patch (id UInt64, v String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0,
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        apply_patches_on_merge = 1;

    SYSTEM STOP MERGES t_merge_mem_implicit_default_patch;
    INSERT INTO t_merge_mem_implicit_default_patch VALUES (1, 'x'), (2, 'y');
    INSERT INTO t_merge_mem_implicit_default_patch VALUES (3, 'z'), (4, 'w');
    ALTER TABLE t_merge_mem_implicit_default_patch ADD COLUMN a String;

    SET enable_lightweight_update = 1;
    UPDATE t_merge_mem_implicit_default_patch SET a = 'patched_payload' WHERE id = 2;

    SYSTEM START MERGES t_merge_mem_implicit_default_patch;
    OPTIMIZE TABLE t_merge_mem_implicit_default_patch FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merge_mem_implicit_default_patch' AND active AND startsWith(name, 'all_');
    SELECT id, a FROM t_merge_mem_implicit_default_patch ORDER BY id;
" -- --merges_mutations_memory_usage_soft_limit=1
