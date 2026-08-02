-- Tags: no-random-merge-tree-settings
-- The arms below pin the merge-tree settings that decide whether a colliding stream exists at all
-- (part type, sparse ratio, packing threshold, hashing), so runner injection would invert them.

SET mutations_sync = 2;

-- Plain: `skp_idx_` is a prefix convention, not a reserved namespace, and escapeForFileName keeps
-- `_` and alphanumerics, so this collides at default settings.
CREATE TABLE t_plain (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_plain SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
SELECT 'plain', count() FROM t_plain;
DROP TABLE t_plain;

-- Same pair with index-name escaping off: the index base is unchanged either way.
CREATE TABLE t_esc (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0;
INSERT INTO t_esc SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_esc;

-- A sparse column's offsets stream is `<base>.sparse.idx`, chosen only after the data is seen, so
-- DDL-time enumeration cannot know this name exists.
CREATE TABLE t_sparse (k UInt64, `skp_idx_a` UInt64 CODEC(NONE), s String,
    INDEX `a.sparse.idx`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0,
         ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO t_sparse SELECT number, 0, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_sparse;

-- `Tuple` picks a serialization kind per ELEMENT, so the colliding stream is a subcolumn's.
CREATE TABLE t_tuple (k UInt64, `skp_idx_t` Tuple(a UInt64, b String) CODEC(NONE), s String,
    INDEX `t%2Ea.sparse.idx`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0,
         ratio_of_defaults_for_sparse_serialization = 0.0;
INSERT INTO t_tuple SELECT number, (0, ''), toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_tuple;

-- `Dynamic` emits `.variant_discr` only for variants actually present in the data.
SET allow_experimental_dynamic_type = 1;
CREATE TABLE t_dyn (k UInt64, `skp_idx_a` Dynamic, s String,
    INDEX `a.variant_discr`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0;
INSERT INTO t_dyn SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_dyn;

-- Both sides go through replaceFileNameToHashIfNeeded, so the comparison is on the post-hash name.
CREATE TABLE t_hash (k UInt64, `skp_idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` UInt64, s String,
    INDEX `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         replace_long_file_name_to_hash = 1, max_file_name_length = 127;
INSERT INTO t_hash SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_hash;

-- A substream that spills out of `skp_idx.packed` takes the directory entry.
CREATE TABLE t_spill (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, packed_skip_index_max_bytes = 1;
INSERT INTO t_spill SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_spill;

-- A substream that stays inside `skp_idx.packed` still owns the base: reads resolve `skp_idx_*`
-- archive keys before the real disk, so the archive member shadows the column's own file.
CREATE TABLE t_packed (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 1000000;
INSERT INTO t_packed SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_packed;

-- ... but a HASHED on-disk name is bare hex, which the archive lookup never matches, so nothing can
-- be shadowed and this pair is legal. This is why the archive claim uses the logical name.
CREATE TABLE t_packed_hash (k UInt64, `skp_idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa` UInt64, s String,
    INDEX `aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 1000000,
         replace_long_file_name_to_hash = 1, max_file_name_length = 20;
INSERT INTO t_packed_hash SELECT number, number, toString(number) FROM numbers(10);
SELECT 'packed-hashed-legal', count(), sum(`skp_idx_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa`) FROM t_packed_hash;
DROP TABLE t_packed_hash;

-- `.sparse` alone is never a filename (getFileNameForStream passes encode_sparse_stream = false),
-- so this pair is legal and must keep working.
CREATE TABLE t_min (k UInt64, `skp_idx_a` UInt64, s String, INDEX `a.sparse`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0;
INSERT INTO t_min SELECT number, number, toString(number) FROM numbers(10);
SELECT 'minimality-pin', count(), sum(`skp_idx_a`) FROM t_min;
DROP TABLE t_min;

-- All Compact columns share one `data.bin`, so no Compact column base can equal a skip-index base.
CREATE TABLE t_compact (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_compact SELECT number, number, toString(number) FROM numbers(10);
SELECT 'compact-legal', count(), sum(`skp_idx_a`) FROM t_compact;
DROP TABLE t_compact;

-- A vertical merge puts the column and the index on different writers, so a per-writer registry
-- could not see the pair.
CREATE TABLE t_vert (k UInt64, `skp_idx_a` UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;
INSERT INTO t_vert SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_vert;

-- Text indices are written by MergeTextIndexesTask, which is a third producer over the directory.
SET allow_experimental_full_text_index = 1;
CREATE TABLE t_text (k UInt64, `skp_idx_a` UInt64, s String,
    INDEX a(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_text SELECT number, number, toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_text;

-- An ordinary text index must still build and merge: the temporary per-segment streams must not
-- claim anything in the part registry.
CREATE TABLE t_text_ok (k UInt64, v UInt64, s String,
    INDEX a(s) TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_text_ok SELECT number, number, toString(number) FROM numbers(10);
INSERT INTO t_text_ok SELECT number + 10, number, toString(number + 10) FROM numbers(10);
OPTIMIZE TABLE t_text_ok FINAL;
SELECT 'text-legal', count(), countIf(hasToken(s, '5')) FROM t_text_ok;
CHECK TABLE t_text_ok SETTINGS check_query_single_value_result = 1;
DROP TABLE t_text_ok;

-- A projection is its own directory with its own registry, so a projection column may be named after
-- a TABLE-ROOT index. Rejecting this would break a legal table.
CREATE TABLE t_proj (k UInt64, x UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1,
    PROJECTION p (SELECT x AS `skp_idx_a`, sum(k) GROUP BY x))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_proj SELECT number, number, toString(number) FROM numbers(10);
SELECT 'projection-cross-directory-legal', count() FROM t_proj;
CHECK TABLE t_proj SETTINGS check_query_single_value_result = 1;
DROP TABLE t_proj;

-- ... while a collision INSIDE the projection's own directory is caught, here between a projection
-- column and the implicit minmax index the projection's own settings add.
CREATE TABLE t_proj_collide (k UInt64, v UInt64, `skp_idx_auto_minmax_index_v` UInt64,
    PROJECTION p (SELECT v, `skp_idx_auto_minmax_index_v` ORDER BY v)
        WITH SETTINGS (add_minmax_index_for_numeric_columns = 1))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_proj_collide SELECT number, number, number FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_proj_collide;

-- ADD COLUMN is metadata-only, so a mutation is how an existing table reaches this state. Before
-- this check the index's marks file was hardlinked onto the column's base, CHECK TABLE reported OK,
-- and only reading the column revealed the corruption.
CREATE TABLE t_carry (k UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_carry SELECT number, toString(number) FROM numbers(10);
ALTER TABLE t_carry ADD COLUMN `skp_idx_a` UInt64 DEFAULT 7;
ALTER TABLE t_carry UPDATE `skp_idx_a` = 5 WHERE 1; -- { serverError UNFINISHED }
SELECT 't_carry', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry';
DROP TABLE t_carry;

-- The carry helper runs before any bytes move, so copying behaves like hardlinking.
CREATE TABLE t_carry_copy (k UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         always_use_copy_instead_of_hardlinks = 1;
INSERT INTO t_carry_copy SELECT number, toString(number) FROM numbers(10);
ALTER TABLE t_carry_copy ADD COLUMN `skp_idx_a` UInt64 DEFAULT 7;
ALTER TABLE t_carry_copy UPDATE `skp_idx_a` = 5 WHERE 1; -- { serverError UNFINISHED }
SELECT 't_carry_copy', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry_copy';
DROP TABLE t_carry_copy;

-- A carried archive member is claimed under its logical name, which is what a read resolves.
CREATE TABLE t_carry_packed (k UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         packed_skip_index_max_bytes = 1000000;
INSERT INTO t_carry_packed SELECT number, toString(number) FROM numbers(10);
ALTER TABLE t_carry_packed ADD COLUMN `skp_idx_a` UInt64 DEFAULT 7;
ALTER TABLE t_carry_packed UPDATE `skp_idx_a` = 5 WHERE 1; -- { serverError UNFINISHED }
SELECT 't_carry_packed', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry_packed';
DROP TABLE t_carry_packed;

-- A mutation that rewrites every column carries indices through its own loops, at a different site.
CREATE TABLE t_carry_full (k UInt64, s String, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_carry_full SELECT number, toString(number) FROM numbers(10);
ALTER TABLE t_carry_full ADD COLUMN `skp_idx_a` UInt64 DEFAULT 7;
ALTER TABLE t_carry_full DELETE WHERE k = 3; -- { serverError UNFINISHED }
SELECT 't_carry_full', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry_full';
DROP TABLE t_carry_full;

-- RENAME COLUMN into a colliding name is claimed from the typed rename planning, not from the
-- `from -> to` filename map, which also carries drops.
CREATE TABLE t_rename (k UInt64, s String, other UInt64, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rename SELECT number, toString(number), number FROM numbers(10);
ALTER TABLE t_rename RENAME COLUMN other TO `skp_idx_a`; -- { serverError UNFINISHED }
SELECT 't_rename', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_rename';
DROP TABLE t_rename;

CREATE TABLE t_rename_ok (k UInt64, s String, other UInt64, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rename_ok SELECT number, toString(number), number FROM numbers(10);
ALTER TABLE t_rename_ok RENAME COLUMN other TO renamed;
SELECT 'rename-legal', count(), sum(renamed) FROM t_rename_ok;
DROP TABLE t_rename_ok;

-- An ordinary mutation carries the whole source directory, so an over-broad claim would break every
-- mutation in the product.
CREATE TABLE t_mut_ok (k UInt64, s String, v UInt64, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_mut_ok SELECT number, toString(number), number FROM numbers(10);
ALTER TABLE t_mut_ok UPDATE v = 5 WHERE 1;
SELECT 'mutation-legal', count(), sum(v) FROM t_mut_ok;
CHECK TABLE t_mut_ok SETTINGS check_query_single_value_result = 1;
DROP TABLE t_mut_ok;

CREATE TABLE t_mut_ok_copy (k UInt64, s String, v UInt64, INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         always_use_copy_instead_of_hardlinks = 1;
INSERT INTO t_mut_ok_copy SELECT number, toString(number), number FROM numbers(10);
ALTER TABLE t_mut_ok_copy UPDATE v = 5 WHERE 1;
SELECT 'mutation-legal-copy', count(), sum(v) FROM t_mut_ok_copy;
CHECK TABLE t_mut_ok_copy SETTINGS check_query_single_value_result = 1;
DROP TABLE t_mut_ok_copy;

-- A carried projection directory is a different namespace from the root one.
CREATE TABLE t_proj_carry (k UInt64, x UInt64, s String, v UInt64,
    INDEX a(s) TYPE set(100) GRANULARITY 1,
    PROJECTION p (SELECT x AS `skp_idx_a`, sum(k) GROUP BY x))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_proj_carry SELECT number, number, toString(number), number FROM numbers(10);
ALTER TABLE t_proj_carry UPDATE v = 5 WHERE 1;
SELECT 'projection-carry-legal', count(), sum(v) FROM t_proj_carry;
CHECK TABLE t_proj_carry SETTINGS check_query_single_value_result = 1;
DROP TABLE t_proj_carry;

-- Statistics drops reach the rename planning with an empty destination.
SET allow_experimental_statistics = 1;
CREATE TABLE t_stats (k UInt64, s String, v UInt64 STATISTICS(tdigest),
    INDEX a(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_stats SELECT number, toString(number), number FROM numbers(10);
ALTER TABLE t_stats MODIFY COLUMN v UInt64;
SELECT 'statistics-drop-legal', count(), sum(v) FROM t_stats;
CHECK TABLE t_stats SETTINGS check_query_single_value_result = 1;
DROP TABLE t_stats;

-- Two columns of one Nested group legitimately share the array-sizes base, so that direction stays
-- owned by the Wide writer's own columns-vs-columns check.
CREATE TABLE t_nested_ok (k UInt64, nested Nested(a UInt32, b UInt32))
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_nested_ok SELECT number, [1, 2], [3, 4] FROM numbers(10);
ALTER TABLE t_nested_ok RENAME COLUMN nested.a TO nested.aa, RENAME COLUMN nested.b TO nested.bb;
SELECT 'nested-multi-rename-legal', count(), sum(nested.aa[1]), sum(nested.bb[1]) FROM t_nested_ok;
DROP TABLE t_nested_ok;

-- The relaxation above must not free that shared base for an index to land on.
CREATE TABLE t_nested_collide (k UInt64, `skp_idx_nested` Nested(a UInt32, b UInt32), s String,
    INDEX `nested.size0`(s) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, escape_index_filenames = 0;
INSERT INTO t_nested_collide SELECT number, [1, 2], [3, 4], toString(number) FROM numbers(10); -- { serverError INCORRECT_FILE_NAME }
DROP TABLE t_nested_collide;

-- Materializing an index onto a column this mutation only carries: the writer claims the index, so
-- the carried column must be claimed too or the two silently share one marks file.
CREATE TABLE t_carry_col (k UInt64, `skp_idx_a` UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_carry_col SELECT number, number, toString(number) FROM numbers(10);
ALTER TABLE t_carry_col ADD INDEX a(s) TYPE set(100) GRANULARITY 1;
ALTER TABLE t_carry_col MATERIALIZE INDEX a; -- { serverError UNFINISHED }
SELECT 't_carry_col', countIf(latest_fail_reason LIKE '%INCORRECT_FILE_NAME%'
    AND latest_fail_reason LIKE '%skip index `a`%'
    AND latest_fail_reason LIKE '%column `skp_idx_a`%')
FROM system.mutations WHERE database = currentDatabase() AND table = 't_carry_col';
DROP TABLE t_carry_col;
