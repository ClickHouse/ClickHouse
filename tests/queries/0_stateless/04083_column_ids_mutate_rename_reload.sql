-- Tags: no-parallel, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- Column-ID state across part reloads (DETACH/ATTACH) and rewrites that follow
-- metadata-only ALTERs.  Canonical table: MergeTree ORDER BY a, SETTINGS
-- serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0,
-- min_rows_for_wide_part = 0; sections vary only their deltas.

SET allow_experimental_column_ids = 1;

-- why: finalizeMutatedPart must write columns.txt with column IDs; a logical-name
-- columns.txt makes the reloaded part unresolvable and a later merge drops the column.
CREATE TABLE t_mutate_rename_reload (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
INSERT INTO t_mutate_rename_reload VALUES (1, 'x', 1.5);
ALTER TABLE t_mutate_rename_reload DROP COLUMN c;
ALTER TABLE t_mutate_rename_reload ADD COLUMN c Float64;
INSERT INTO t_mutate_rename_reload VALUES (2, 'y', 9.9);
SELECT 'before_mutation', column, column_id != column AS is_non_identity
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_rename_reload' AND active AND column = 'c'
ORDER BY name;
ALTER TABLE t_mutate_rename_reload UPDATE c = 99.9 WHERE a = 2 SETTINGS mutations_sync = 1;
ALTER TABLE t_mutate_rename_reload RENAME COLUMN c TO price;
DETACH TABLE t_mutate_rename_reload SYNC;
ATTACH TABLE t_mutate_rename_reload;
SELECT 'after_restart', a, price FROM t_mutate_rename_reload ORDER BY a;
OPTIMIZE TABLE t_mutate_rename_reload FINAL;
SELECT 'after_merge', column, column_id != column AS is_non_identity
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_rename_reload' AND active AND column = 'price'
ORDER BY name;
SELECT 'final', a, price FROM t_mutate_rename_reload ORDER BY a;
DROP TABLE t_mutate_rename_reload SYNC;

-- why: a wide-part partial mutation right after a metadata-only RENAME (no reload)
-- must key source-slot lookups by ID, not the part's stale load-time col.name.
CREATE TABLE t_mutate_after_rename (a UInt32, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0;
INSERT INTO t_mutate_after_rename VALUES (1, 'hello', 1.5), (2, 'world', 2.5);
ALTER TABLE t_mutate_after_rename RENAME COLUMN b TO d;
ALTER TABLE t_mutate_after_rename UPDATE c = c + 100 WHERE 1 SETTINGS mutations_sync = 1;
SELECT 'after_mutation', a, d, c FROM t_mutate_after_rename ORDER BY a;
SELECT 'columns', column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_mutate_after_rename' AND active
ORDER BY column;
INSERT INTO t_mutate_after_rename (a, c, d) VALUES (3, 3.5, 'after');
OPTIMIZE TABLE t_mutate_after_rename FINAL;
SELECT 'after_merge', a, d, c FROM t_mutate_after_rename ORDER BY a;
DROP TABLE t_mutate_after_rename SYNC;

-- why: a pre-activation compact part must keep its stale dropped column in the ordinal
-- slot after DROP + re-ADD, or the sibling column reads the wrong bytes.
CREATE TABLE t_compact_dropadd (a UInt64, b String, c Float64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 1000000000,
         min_rows_for_wide_part = 1000000000;
INSERT INTO t_compact_dropadd VALUES (1, 'x', 1.5), (2, 'y', 9.9);
ALTER TABLE t_compact_dropadd DROP COLUMN b;
ALTER TABLE t_compact_dropadd ADD COLUMN b String DEFAULT 'def_b';
SELECT a, b, c FROM t_compact_dropadd ORDER BY a;
SELECT count() AS bad_rows
FROM system.parts_columns
WHERE database = currentDatabase()
  AND table = 't_compact_dropadd'
  AND active
  AND column_id = ''
  AND NOT startsWith(column, '_');
DROP TABLE t_compact_dropadd SYNC;

-- why: serialization-info records of a non-identity-ID column must survive write,
-- reload, mutation and merge keyed by the stamped ID -- a record lost in re-keying
-- silently downgrades the column's serialization kind.
CREATE TABLE t_readd_sparse (a UInt32, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9;
ALTER TABLE t_readd_sparse DROP COLUMN c;
ALTER TABLE t_readd_sparse ADD COLUMN c UInt64; -- re-added c gets counter ID '1'
INSERT INTO t_readd_sparse SELECT number, if(number = 7, 9, 0), if(number = 5, 7, 0) FROM numbers(1000);
SELECT 'kinds_before_reload', column, column_id, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_sparse' AND active AND column IN ('b', 'c')
ORDER BY column;
SELECT 'values_before_reload', sum(b), sum(c) FROM t_readd_sparse;
DETACH TABLE t_readd_sparse SYNC;
ATTACH TABLE t_readd_sparse;
SELECT 'kinds_after_reload', column, column_id, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_sparse' AND active AND column IN ('b', 'c')
ORDER BY column;
SELECT 'values_after_reload', sum(b), sum(c) FROM t_readd_sparse;
ALTER TABLE t_readd_sparse UPDATE c = c + 1 WHERE a = 9 SETTINGS mutations_sync = 1;
SELECT 'values_after_mutation', sum(b), sum(c) FROM t_readd_sparse;
OPTIMIZE TABLE t_readd_sparse FINAL;
SELECT 'kinds_after_merge', column, column_id, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_readd_sparse' AND active AND column IN ('b', 'c')
ORDER BY column;
SELECT 'values_after_merge', sum(b), sum(c) FROM t_readd_sparse;
DROP TABLE t_readd_sparse SYNC;

-- why: statistics files are named by the stamped column ID and matched ID-first;
-- name-based files silently lose the statistics after a RENAME plus part reload.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
CREATE TABLE t_ids_stats (a UInt64, v UInt64 STATISTICS(basic))
ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0,
         auto_statistics_types = '';
INSERT INTO t_ids_stats SELECT number, number * 10 FROM numbers(100);
SELECT name, column, statistics FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_stats' AND active AND column = 'v';
ALTER TABLE t_ids_stats RENAME COLUMN v TO w;
DETACH TABLE t_ids_stats;
ATTACH TABLE t_ids_stats;
SELECT name, column, statistics FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_stats' AND active AND column = 'w';
ALTER TABLE t_ids_stats DROP COLUMN w;
ALTER TABLE t_ids_stats ADD COLUMN w UInt64;
ALTER TABLE t_ids_stats ADD STATISTICS w TYPE basic;
INSERT INTO t_ids_stats SELECT number, number * 7 FROM numbers(100, 100);
DETACH TABLE t_ids_stats;
ATTACH TABLE t_ids_stats;
SELECT name, column, column_id, statistics FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_stats' AND active AND column = 'w' AND name = 'all_2_2_0';
DROP TABLE t_ids_stats SYNC;
