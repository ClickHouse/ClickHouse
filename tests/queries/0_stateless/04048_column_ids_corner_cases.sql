-- Tags: no-parallel, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings, no-object-storage
-- why: column-ID corner cases -- metadata-only RENAME/DROP mechanics, rejected ALTERs, TTL, partition transfer, projections, Nested.
-- The later sections (marks/minmax/byte-size introspection) assert on local part layout, hence no-object-storage / no-parallel-replicas.

SET allow_experimental_column_ids = 1;
SET mutations_sync = 1;

-- why: chained renames must keep resolving to the same column ID.
CREATE TABLE t_ids_chain (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_chain VALUES (1, 'hello');
INSERT INTO t_ids_chain VALUES (2, 'world');
ALTER TABLE t_ids_chain RENAME COLUMN b TO d;
SELECT a, d FROM t_ids_chain ORDER BY a;
ALTER TABLE t_ids_chain RENAME COLUMN d TO e;
SELECT a, e FROM t_ids_chain ORDER BY a;
ALTER TABLE t_ids_chain RENAME COLUMN e TO f;
SELECT a, f FROM t_ids_chain ORDER BY a;
OPTIMIZE TABLE t_ids_chain FINAL;
SELECT a, f FROM t_ids_chain ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_chain' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_chain SYNC;

-- why: ADD + RENAME + DROP in one ALTER must apply atomically to the mapping.
CREATE TABLE t_ids_multi_op (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_multi_op VALUES (1, 'one', 10);
INSERT INTO t_ids_multi_op VALUES (2, 'two', 20);
ALTER TABLE t_ids_multi_op
    ADD COLUMN d Float64 DEFAULT 3.14,
    RENAME COLUMN b TO name,
    DROP COLUMN c;
SELECT a, name, d FROM t_ids_multi_op ORDER BY a;
INSERT INTO t_ids_multi_op VALUES (3, 'three', 2.5);
SELECT a, name, d FROM t_ids_multi_op ORDER BY a;
OPTIMIZE TABLE t_ids_multi_op FINAL;
SELECT a, name, d FROM t_ids_multi_op ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_multi_op' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_multi_op SYNC;

-- why: RENAME, DROP of the renamed column, then re-ADD of the original name must not collide IDs.
CREATE TABLE t_ids_rename_drop (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_rename_drop VALUES (1, 'one', 10);
ALTER TABLE t_ids_rename_drop RENAME COLUMN b TO d;
ALTER TABLE t_ids_rename_drop DROP COLUMN d;
SELECT a, c FROM t_ids_rename_drop ORDER BY a;
ALTER TABLE t_ids_rename_drop ADD COLUMN d String DEFAULT 'new';
INSERT INTO t_ids_rename_drop VALUES (2, 20, 'added');
SELECT a, c, d FROM t_ids_rename_drop ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_rename_drop' AND active AND column = 'd' ORDER BY column, column_id;
DROP TABLE t_ids_rename_drop SYNC;

-- why: Nullable columns carry an extra null-map stream that must follow the ID too.
CREATE TABLE t_ids_nullable (a UInt64, b Nullable(String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nullable VALUES (1, 'hello');
INSERT INTO t_ids_nullable VALUES (2, NULL);
INSERT INTO t_ids_nullable VALUES (3, 'world');
ALTER TABLE t_ids_nullable RENAME COLUMN b TO d;
SELECT a, d FROM t_ids_nullable ORDER BY a;
OPTIMIZE TABLE t_ids_nullable FINAL;
SELECT a, d FROM t_ids_nullable ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_nullable' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_nullable SYNC;

-- why: mapping updates must work with no data parts at all.
CREATE TABLE t_ids_empty (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_empty RENAME COLUMN b TO d;
ALTER TABLE t_ids_empty ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE t_ids_empty DROP COLUMN c;
INSERT INTO t_ids_empty VALUES (1, 'after_rename');
SELECT a, d FROM t_ids_empty ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_empty' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_empty SYNC;

-- why: the ID counter must never reuse IDs freed by DROP.
CREATE TABLE t_ids_counter (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_counter ADD COLUMN c1 UInt64 DEFAULT 0;
ALTER TABLE t_ids_counter ADD COLUMN c2 UInt64 DEFAULT 0;
ALTER TABLE t_ids_counter ADD COLUMN c3 UInt64 DEFAULT 0;
ALTER TABLE t_ids_counter DROP COLUMN c1;
ALTER TABLE t_ids_counter DROP COLUMN c2;
ALTER TABLE t_ids_counter DROP COLUMN c3;
ALTER TABLE t_ids_counter ADD COLUMN d1 UInt64 DEFAULT 0;
ALTER TABLE t_ids_counter ADD COLUMN d2 UInt64 DEFAULT 0;
INSERT INTO t_ids_counter VALUES (1, 10, 20);
SELECT a, d1, d2 FROM t_ids_counter ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_counter' AND active AND column LIKE 'd%' ORDER BY column, column_id;
DROP TABLE t_ids_counter SYNC;

-- why: RENAME with an active mapping must be metadata-only -- no mutation.
CREATE TABLE t_ids_instant_rename (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_instant_rename VALUES (1, 'one');
INSERT INTO t_ids_instant_rename VALUES (2, 'two');
ALTER TABLE t_ids_instant_rename RENAME COLUMN b TO d;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_instant_rename' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_instant_rename' AND NOT is_done;
SELECT a, d FROM t_ids_instant_rename ORDER BY a;
INSERT INTO t_ids_instant_rename VALUES (3, 'three');
SELECT a, d FROM t_ids_instant_rename ORDER BY a;
ALTER TABLE t_ids_instant_rename RENAME COLUMN d TO e;
SELECT a, e FROM t_ids_instant_rename ORDER BY a;
OPTIMIZE TABLE t_ids_instant_rename FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_instant_rename' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, e FROM t_ids_instant_rename ORDER BY a;
DROP TABLE t_ids_instant_rename SYNC;

-- why: DROP with an active mapping must be metadata-only; the merge cleans dropped files up.
CREATE TABLE t_ids_instant_drop (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_instant_drop VALUES (1, 'one', 10);
ALTER TABLE t_ids_instant_drop DROP COLUMN c;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_instant_drop' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_instant_drop' AND NOT is_done;
SELECT * FROM t_ids_instant_drop ORDER BY a;
INSERT INTO t_ids_instant_drop VALUES (2, 'two');
OPTIMIZE TABLE t_ids_instant_drop FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_instant_drop' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT * FROM t_ids_instant_drop ORDER BY a;
DROP TABLE t_ids_instant_drop SYNC;

-- why: an existing table activates lazily on the first compatible ALTER; later RENAMEs are instant.
CREATE TABLE t_ids_lazy_activate (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_lazy_activate VALUES (1, 'one');
ALTER TABLE t_ids_lazy_activate MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_lazy_activate ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO t_ids_lazy_activate (a, b, c) VALUES (2, 'two', 22);
ALTER TABLE t_ids_lazy_activate RENAME COLUMN b TO d;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_lazy_activate' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_ids_lazy_activate' AND NOT is_done;
SELECT a, d, c FROM t_ids_lazy_activate ORDER BY a;
DROP TABLE t_ids_lazy_activate SYNC;

-- why: MODIFY COLUMN is a data rewrite and must still go through a mutation.
CREATE TABLE t_ids_modify_mutates (a UInt64, b UInt32) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_modify_mutates VALUES (1, 10);
ALTER TABLE t_ids_modify_mutates MODIFY COLUMN b UInt64;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_modify_mutates' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT b, toTypeName(b) FROM t_ids_modify_mutates;
DROP TABLE t_ids_modify_mutates SYNC;

-- why: renaming a counter-ID column must keep parts written before the rename readable
-- (columns.txt stores IDs, not logical names).
CREATE TABLE t_rename_non_identity (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_rename_non_identity VALUES (1);
ALTER TABLE t_rename_non_identity ADD COLUMN b String DEFAULT 'dflt';
INSERT INTO t_rename_non_identity (a, b) VALUES (2, 'hello');
ALTER TABLE t_rename_non_identity RENAME COLUMN b TO c;
SELECT a, c FROM t_rename_non_identity ORDER BY a;
INSERT INTO t_rename_non_identity (a, c) VALUES (3, 'world');
OPTIMIZE TABLE t_rename_non_identity FINAL;
SELECT a, c FROM t_rename_non_identity ORDER BY a;
DROP TABLE t_rename_non_identity SYNC;

-- why: a metadata-only RENAME of a sparse numeric-ID column must not invalidate the
-- part's in-memory serialization records -- same-session reads must stay Sparse.
CREATE TABLE t_sparse_rename (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_sparse_rename ADD COLUMN s String;
INSERT INTO t_sparse_rename SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(1000);
ALTER TABLE t_sparse_rename RENAME COLUMN s TO s2;
SELECT countIf(s2 != ''), count() FROM t_sparse_rename;
SELECT a, s2 FROM t_sparse_rename WHERE s2 != '' ORDER BY a LIMIT 3;
DROP TABLE t_sparse_rename SYNC;

-- why: with optimize_functions_to_subcolumns=1, `s2 != ''` is rewritten into a read of the
-- Sparse column's SIZE subcolumn (s2.size). After a metadata-only RENAME the part still holds the
-- load-time name, so the subcolumn size must resolve by the stable id -- resolving by the renamed
-- name misses the part and throws NO_SUCH_COLUMN_IN_TABLE. Keep the optimization ON.
CREATE TABLE t_sparse_rename_subcolumn (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 0,
    serialization_info_version = 'with_column_ids',
    ratio_of_defaults_for_sparse_serialization = 0.5;
ALTER TABLE t_sparse_rename_subcolumn ADD COLUMN s String;
INSERT INTO t_sparse_rename_subcolumn SELECT number, if(number % 100 = 0, 'x', '') FROM numbers(1000);
ALTER TABLE t_sparse_rename_subcolumn RENAME COLUMN s TO s2;
SELECT countIf(s2 != ''), count() FROM t_sparse_rename_subcolumn SETTINGS optimize_functions_to_subcolumns = 1;
SELECT a, s2 FROM t_sparse_rename_subcolumn WHERE s2 != '' ORDER BY a LIMIT 3 SETTINGS optimize_functions_to_subcolumns = 1;
DROP TABLE t_sparse_rename_subcolumn SYNC;

-- why: after DROP c(String) + ADD c(UInt64), the old part's same-named dead String
-- column has a different ID and must not contribute its type to the read.
CREATE TABLE t_drop_add_type (a UInt64, c String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_drop_add_type SELECT number, 'old' FROM numbers(1000);
ALTER TABLE t_drop_add_type DROP COLUMN c;
ALTER TABLE t_drop_add_type ADD COLUMN c UInt64;
INSERT INTO t_drop_add_type SELECT number + 1000, number + 1000 FROM numbers(1000);
SELECT sum(c), count() FROM t_drop_add_type;
SELECT a, c FROM t_drop_add_type ORDER BY a LIMIT 2;
SELECT a, c FROM t_drop_add_type ORDER BY a DESC LIMIT 2;
DROP TABLE t_drop_add_type SYNC;

-- why: DETACH/ATTACH PARTITION must re-resolve part columns through the current mapping.
CREATE TABLE t_ids_detach (a UInt64, b String) ENGINE = MergeTree PARTITION BY a ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_detach VALUES (1, 'hello');
INSERT INTO t_ids_detach VALUES (2, 'world');
ALTER TABLE t_ids_detach RENAME COLUMN b TO d;
ALTER TABLE t_ids_detach DETACH PARTITION 1;
SELECT a, d FROM t_ids_detach ORDER BY a;
ALTER TABLE t_ids_detach ATTACH PARTITION 1;
SELECT a, d FROM t_ids_detach ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_detach' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_detach SYNC;

-- why: Map/Tuple columns and a renamed counter-ID sibling must survive a merge together.
CREATE TABLE t_ids_complex_types (a UInt64, b Map(String, UInt64), c Tuple(x UInt64, y String))
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_complex_types VALUES (1, {'k1': 10, 'k2': 20}, (100, 'hello'));
INSERT INTO t_ids_complex_types VALUES (2, {'k3': 30}, (200, 'world'));
ALTER TABLE t_ids_complex_types ADD COLUMN d String DEFAULT 'extra';
ALTER TABLE t_ids_complex_types RENAME COLUMN d TO e;
SELECT a, b, c, e FROM t_ids_complex_types ORDER BY a;
OPTIMIZE TABLE t_ids_complex_types FINAL;
SELECT a, b, c, e FROM t_ids_complex_types ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_complex_types' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_complex_types SYNC;

-- why: OPTIMIZE DEDUPLICATE BY the renamed name must resolve to the right streams.
CREATE TABLE t_ids_dedup (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_dedup VALUES (1, 'hello');
INSERT INTO t_ids_dedup VALUES (1, 'hello');
INSERT INTO t_ids_dedup VALUES (2, 'world');
ALTER TABLE t_ids_dedup RENAME COLUMN b TO d;
OPTIMIZE TABLE t_ids_dedup FINAL DEDUPLICATE BY a, d;
SELECT a, d FROM t_ids_dedup ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_dedup' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
DROP TABLE t_ids_dedup SYNC;

-- why: a DEFAULT expression referencing a renamed column must keep evaluating.
CREATE TABLE t_ids_defaults (a UInt64, b UInt64, c UInt64 DEFAULT b * 2) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_defaults (a, b) VALUES (1, 10);
SELECT a, b, c FROM t_ids_defaults ORDER BY a;
ALTER TABLE t_ids_defaults RENAME COLUMN b TO val;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_defaults' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
INSERT INTO t_ids_defaults (a, val) VALUES (2, 20);
SELECT a, val, c FROM t_ids_defaults ORDER BY a;
DROP TABLE t_ids_defaults SYNC;

-- why: single-ALTER DROP + re-ADD of one name cannot be made crash-safe between the
-- metadata commit and the cleanup mutation, so it is rejected regardless of
-- allow_non_metadata_alters; two separate ALTERs are the workaround.
CREATE TABLE t_ids_drop_readd (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_drop_readd VALUES (1, 'old_data');
ALTER TABLE t_ids_drop_readd DROP COLUMN b, ADD COLUMN b String DEFAULT 'new_default'; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_ids_drop_readd DROP COLUMN b, ADD COLUMN b String DEFAULT 'new_default'
SETTINGS allow_non_metadata_alters = 0; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_ids_drop_readd DROP COLUMN b;
ALTER TABLE t_ids_drop_readd ADD COLUMN b String DEFAULT 'new_default';
INSERT INTO t_ids_drop_readd VALUES (2, 'inserted');
SELECT a, b FROM t_ids_drop_readd ORDER BY a;
SELECT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_drop_readd' AND active AND column = 'b' AND NOT startsWith(column, '_')
    ORDER BY column, name;
OPTIMIZE TABLE t_ids_drop_readd FINAL;
SELECT a, b FROM t_ids_drop_readd ORDER BY a;
DROP TABLE t_ids_drop_readd SYNC;

-- why: same rejection for a Nested column.
CREATE TABLE t_ids_drop_readd_nested (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_drop_readd_nested VALUES (1, [10, 20], ['a', 'b']);
ALTER TABLE t_ids_drop_readd_nested DROP COLUMN n, ADD COLUMN n Nested(x UInt64, y String); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_ids_drop_readd_nested DROP COLUMN n;
ALTER TABLE t_ids_drop_readd_nested ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_ids_drop_readd_nested VALUES (2, [30, 40], ['c', 'd']);
SELECT a, `n.x`, `n.y` FROM t_ids_drop_readd_nested ORDER BY a;
OPTIMIZE TABLE t_ids_drop_readd_nested FINAL;
SELECT a, `n.x`, `n.y` FROM t_ids_drop_readd_nested ORDER BY a;
DROP TABLE t_ids_drop_readd_nested SYNC;

-- why: a single-child Nested column ADDed after activation has a plain counter ID;
-- renaming it across parent boundaries would break offset stream lookup, so reject.
CREATE TABLE t_ids_nested_single (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_nested_single ADD COLUMN `n.x` Array(UInt64);
INSERT INTO t_ids_nested_single VALUES (1, [10, 20, 30]);
ALTER TABLE t_ids_nested_single RENAME COLUMN `n.x` TO `m.x`; -- { serverError NOT_IMPLEMENTED }
SELECT a, `n.x` FROM t_ids_nested_single;
DROP TABLE t_ids_nested_single SYNC;

-- why: moving one Nested child to another parent would leave its sibling racing on the
-- shared offsets stream; all siblings must move together in the same ALTER.
CREATE TABLE t_ids_nested_multi (a UInt64, `n.x` Array(UInt64), `n.y` Array(String))
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nested_multi VALUES (1, [10, 20], ['aa', 'bb']);
ALTER TABLE t_ids_nested_multi RENAME COLUMN `n.x` TO `m.x`; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_ids_nested_multi RENAME COLUMN `n.x` TO `m.x`, RENAME COLUMN `n.y` TO `m.y`;
INSERT INTO t_ids_nested_multi VALUES (2, [30], ['cc']);
SELECT a, `m.x`, `m.y` FROM t_ids_nested_multi ORDER BY a;
OPTIMIZE TABLE t_ids_nested_multi FINAL;
SELECT a, `m.x`, `m.y` FROM t_ids_nested_multi ORDER BY a;
DROP TABLE t_ids_nested_multi SYNC;

-- why: once a mapping exists the data files are named by column IDs; ALTERing the
-- settings back must be rejected instead of letting the schema lie about the disk.
CREATE TABLE t_ids_deactivate (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_deactivate VALUES (1, 'x');
ALTER TABLE t_ids_deactivate MODIFY SETTING serialization_info_version = 'with_types'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_ids_deactivate MODIFY SETTING serialization_info_version = 'basic'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_ids_deactivate RESET SETTING serialization_info_version; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE t_ids_deactivate MODIFY SETTING parts_to_throw_insert = 500;
CREATE TABLE t_ids_deactivate_lazy (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_deactivate_lazy VALUES (1, 'x');
ALTER TABLE t_ids_deactivate_lazy MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_deactivate_lazy RENAME COLUMN b TO b2;
ALTER TABLE t_ids_deactivate_lazy MODIFY SETTING serialization_info_version = 'with_types'; -- { serverError SUPPORT_IS_DISABLED }
SELECT a, b FROM t_ids_deactivate ORDER BY a;
SELECT a, b2 FROM t_ids_deactivate_lazy ORDER BY a;
DROP TABLE t_ids_deactivate SYNC;
DROP TABLE t_ids_deactivate_lazy SYNC;

-- why: compact Variant subcolumn streams must be found after toggling
-- escape_variant_subcolumn_filenames across a rename (physical-name retry).
CREATE TABLE t_ids_variant_compact (a UInt64, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY a
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    serialization_info_version = 'with_column_ids',
    escape_variant_subcolumn_filenames = 0;
INSERT INTO t_ids_variant_compact VALUES (1, 'hello'), (2, 42);
SELECT a, v, variantType(v) FROM t_ids_variant_compact ORDER BY a;
ALTER TABLE t_ids_variant_compact MODIFY SETTING escape_variant_subcolumn_filenames = 1;
ALTER TABLE t_ids_variant_compact RENAME COLUMN v TO w;
INSERT INTO t_ids_variant_compact VALUES (3, 'world'), (4, 99);
SELECT a, w, variantType(w) FROM t_ids_variant_compact ORDER BY a;
OPTIMIZE TABLE t_ids_variant_compact FINAL;
SELECT a, w, variantType(w) FROM t_ids_variant_compact ORDER BY a;
DROP TABLE t_ids_variant_compact SYNC;

-- why: column_ids.json must survive a full table DETACH/ATTACH cycle.
CREATE TABLE t_ids_full_detach (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_full_detach VALUES (1, 'before');
ALTER TABLE t_ids_full_detach RENAME COLUMN b TO d;
INSERT INTO t_ids_full_detach (a, d) VALUES (2, 'after');
SELECT a, d FROM t_ids_full_detach ORDER BY a;
DETACH TABLE t_ids_full_detach;
ATTACH TABLE t_ids_full_detach;
SELECT a, d FROM t_ids_full_detach ORDER BY a;
OPTIMIZE TABLE t_ids_full_detach FINAL;
SELECT a, d FROM t_ids_full_detach ORDER BY a;
DROP TABLE t_ids_full_detach SYNC;

-- why: table TTL must keep working when an unrelated column is renamed.
-- INTERVAL 50 YEAR stays within the 32-bit DateTime range.
CREATE TABLE t_ids_ttl (a UInt64, b String, dt DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY a
TTL dt + INTERVAL 50 YEAR
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_ttl (a, b) VALUES (1, 'hello');
ALTER TABLE t_ids_ttl RENAME COLUMN b TO d;
INSERT INTO t_ids_ttl (a, d) VALUES (2, 'world');
SELECT a, d FROM t_ids_ttl ORDER BY a;
OPTIMIZE TABLE t_ids_ttl FINAL;
SELECT a, d FROM t_ids_ttl ORDER BY a;
DROP TABLE t_ids_ttl SYNC;

-- why: per-part columns_ttl entries (ttl.txt) are keyed by the stamped column ID; a
-- name-keyed lookup after a metadata-only RENAME misses the old parts' expired entry
-- and skips clearing the expired values.
CREATE TABLE t_ids_ttl_rename (d DateTime, a UInt64, v String TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
SYSTEM STOP TTL MERGES t_ids_ttl_rename;
INSERT INTO t_ids_ttl_rename VALUES (now(), 1, 'doomed');
SELECT sleep(2) FORMAT Null;
ALTER TABLE t_ids_ttl_rename RENAME COLUMN v TO w;
INSERT INTO t_ids_ttl_rename VALUES (now() + INTERVAL 1 DAY, 2, 'alive');
SYSTEM START TTL MERGES t_ids_ttl_rename;
OPTIMIZE TABLE t_ids_ttl_rename FINAL;
SELECT a, w FROM t_ids_ttl_rename ORDER BY a;
DROP TABLE t_ids_ttl_rename SYNC;

-- why: after DROP + re-ADD, the dropped column's stale ID-keyed TTL entry must not
-- shadow the re-added column's TTL state.
CREATE TABLE t_ids_ttl_readd (d DateTime, a UInt64, v String TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
SYSTEM STOP TTL MERGES t_ids_ttl_readd;
INSERT INTO t_ids_ttl_readd VALUES (now(), 1, 'old_column');
SELECT sleep(2) FORMAT Null;
ALTER TABLE t_ids_ttl_readd DROP COLUMN v;
ALTER TABLE t_ids_ttl_readd ADD COLUMN v String TTL d + INTERVAL 1 YEAR;
INSERT INTO t_ids_ttl_readd VALUES (now(), 2, 'fresh');
SELECT column, column_id, column_ttl_min IS NOT NULL AND column_ttl_max IS NOT NULL AS has_ttl_times
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_ttl_readd' AND active AND column = 'v'
    ORDER BY column_id;
SYSTEM START TTL MERGES t_ids_ttl_readd;
OPTIMIZE TABLE t_ids_ttl_readd FINAL;
SELECT a, v FROM t_ids_ttl_readd ORDER BY a;
SELECT column, column_id, column_ttl_min IS NOT NULL AND column_ttl_max IS NOT NULL AS has_ttl_times
FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_ttl_readd' AND active AND column = 'v'
    ORDER BY column_id;
DROP TABLE t_ids_ttl_readd SYNC;

-- why: ATTACH PARTITION FROM succeeds when mappings and counters agree.
CREATE TABLE t_ids_part_src (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_src DROP COLUMN c;
ALTER TABLE t_ids_part_dst ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_dst DROP COLUMN c;
INSERT INTO t_ids_part_src VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst ATTACH PARTITION 1 FROM t_ids_part_src;
SELECT a, b FROM t_ids_part_dst;
DROP TABLE t_ids_part_src SYNC;
DROP TABLE t_ids_part_dst SYNC;

-- why: a source counter ahead of the destination is rejected -- transferred parts may
-- carry orphan files at IDs the destination would later hand out via ADD COLUMN.
CREATE TABLE t_ids_part_src_b (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst_b (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src_b ADD COLUMN c UInt64;
ALTER TABLE t_ids_part_src_b DROP COLUMN c;
INSERT INTO t_ids_part_src_b VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst_b ATTACH PARTITION 1 FROM t_ids_part_src_b; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ids_part_src_b SYNC;
DROP TABLE t_ids_part_dst_b SYNC;

-- why: diverged logical-to-ID mappings (src b->"1" vs dst b->"b") must be rejected.
CREATE TABLE t_ids_part_src2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
CREATE TABLE t_ids_part_dst2 (a UInt64, b String) ENGINE = MergeTree ORDER BY a PARTITION BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_part_src2 DROP COLUMN b;
ALTER TABLE t_ids_part_src2 ADD COLUMN b String;
INSERT INTO t_ids_part_src2 VALUES (1, 'hello');
ALTER TABLE t_ids_part_dst2 ATTACH PARTITION 1 FROM t_ids_part_src2; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ids_part_src2 SYNC;
DROP TABLE t_ids_part_dst2 SYNC;

-- why: share_nested_offsets = 0 requests per-column offsets; the ID-based stream name
-- must not fold them back onto the Nested parent prefix.
CREATE TABLE t_ids_nested_no_share (a UInt64, n Nested(x UInt32, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, share_nested_offsets = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nested_no_share VALUES (1, [10, 20], ['a', 'bb']);
INSERT INTO t_ids_nested_no_share VALUES (2, [30], ['cc']);
SELECT a, n.x, n.y FROM t_ids_nested_no_share ORDER BY a;
OPTIMIZE TABLE t_ids_nested_no_share FINAL;
SELECT a, n.x, n.y FROM t_ids_nested_no_share ORDER BY a;
DROP TABLE t_ids_nested_no_share SYNC;

-- why: projection parts must survive a parent-column rename and a merge.
CREATE TABLE t_ids_proj (a UInt64, b String, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_proj ADD PROJECTION p_sum (SELECT a, sum(c) GROUP BY a);
INSERT INTO t_ids_proj VALUES (1, 'one', 10);
INSERT INTO t_ids_proj VALUES (1, 'two', 20);
INSERT INTO t_ids_proj VALUES (2, 'three', 30);
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
ALTER TABLE t_ids_proj RENAME COLUMN b TO d;
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
OPTIMIZE TABLE t_ids_proj FINAL;
SELECT DISTINCT column, column_id FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ids_proj' AND active AND NOT startsWith(column, '_') ORDER BY column, column_id;
SELECT a, sum(c) FROM t_ids_proj GROUP BY a ORDER BY a SETTINGS force_optimize_projection = 1;
DROP TABLE t_ids_proj SYNC;

-- why: a flattened Nested ADDed after activation gets compound column IDs ("<n>.x", "<n>.y").
CREATE TABLE t_ids_flat_add (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_flat_add VALUES (1, 'one');
ALTER TABLE t_ids_flat_add ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_ids_flat_add VALUES (2, 'two', [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_add VALUES (3, 'three', [30], ['c']);
SELECT a, b, `n.x`, `n.y` FROM t_ids_flat_add ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_add' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
OPTIMIZE TABLE t_ids_flat_add FINAL;
SELECT a, b, `n.x`, `n.y` FROM t_ids_flat_add ORDER BY a;
DROP TABLE t_ids_flat_add SYNC;

-- why: renaming a field WITHIN a Nested group is metadata-only and keeps the compound ID.
CREATE TABLE t_ids_flat_rename (a UInt64, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_flat_rename ADD COLUMN n Nested(x UInt64, y String);
INSERT INTO t_ids_flat_rename VALUES (1, 'hello', [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_rename VALUES (2, 'world', [30], ['c']);
ALTER TABLE t_ids_flat_rename RENAME COLUMN `n.x` TO `n.z`;
SELECT a, b, `n.z`, `n.y` FROM t_ids_flat_rename ORDER BY a;
OPTIMIZE TABLE t_ids_flat_rename FINAL;
SELECT a, b, `n.z`, `n.y` FROM t_ids_flat_rename ORDER BY a;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_rename' AND active
    AND column LIKE 'n.%'
    ORDER BY column, column_id;
DROP TABLE t_ids_flat_rename SYNC;

-- why: a pre-activation flattened Nested keeps identity IDs after lazy activation.
CREATE TABLE t_ids_flat_existing (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_flat_existing VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_ids_flat_existing VALUES (2, [30], ['c']);
ALTER TABLE t_ids_flat_existing MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_flat_existing ADD COLUMN c UInt64 DEFAULT 0;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_flat_existing' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
ALTER TABLE t_ids_flat_existing RENAME COLUMN c TO d;
SELECT a, `n.x`, `n.y`, d FROM t_ids_flat_existing ORDER BY a;
OPTIMIZE TABLE t_ids_flat_existing FINAL;
SELECT a, `n.x`, `n.y`, d FROM t_ids_flat_existing ORDER BY a;
DROP TABLE t_ids_flat_existing SYNC;

-- why: non-flattened Nested (flatten_nested = 0) must survive a sibling rename and merge.
SET flatten_nested = 0;
CREATE TABLE t_ids_nested_nf (a UInt64, b String, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
INSERT INTO t_ids_nested_nf VALUES (1, 'hello', [(10, 'a'), (20, 'b')]);
INSERT INTO t_ids_nested_nf VALUES (2, 'world', [(30, 'c')]);
ALTER TABLE t_ids_nested_nf RENAME COLUMN b TO d;
SELECT a, d, n.x, n.y FROM t_ids_nested_nf ORDER BY a;
OPTIMIZE TABLE t_ids_nested_nf FINAL;
SELECT a, d, n.x, n.y FROM t_ids_nested_nf ORDER BY a;
DROP TABLE t_ids_nested_nf SYNC;
SET flatten_nested = 1;

-- why: identity-mapped Nested children may move across parents as metadata-only -- the
-- offset stream name derives from the physical prefix, which the rename does not change.
CREATE TABLE t_ids_identity_nested_rename (a UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_ids_identity_nested_rename VALUES (1, [10, 20], ['a', 'b']);
INSERT INTO t_ids_identity_nested_rename VALUES (2, [30], ['c']);
ALTER TABLE t_ids_identity_nested_rename MODIFY SETTING
    serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_identity_nested_rename ADD COLUMN c UInt64 DEFAULT 0;
SELECT DISTINCT column, column_id FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_ids_identity_nested_rename' AND active
    AND column IN ('n.x', 'n.y')
    ORDER BY column, column_id;
ALTER TABLE t_ids_identity_nested_rename RENAME COLUMN `n.x` TO `m.x`, RENAME COLUMN `n.y` TO `m.y`;
SELECT a, `m.x`, `m.y` FROM t_ids_identity_nested_rename ORDER BY a;
OPTIMIZE TABLE t_ids_identity_nested_rename FINAL;
SELECT a, `m.x`, `m.y` FROM t_ids_identity_nested_rename ORDER BY a;
DROP TABLE t_ids_identity_nested_rename SYNC;

-- why: a live column named like another live column's ID would make on-disk keys
-- ambiguous; the stream-collision check must reject every ALTER route to that state.
CREATE TABLE t_ids_name_id_guard (a UInt64, b UInt64, x UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, serialization_info_version = 'with_column_ids';
ALTER TABLE t_ids_name_id_guard RENAME COLUMN b TO d; -- d keeps identity ID 'b'
ALTER TABLE t_ids_name_id_guard ADD COLUMN b UInt64; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_ids_name_id_guard RENAME COLUMN x TO b; -- { serverError BAD_ARGUMENTS }
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't_ids_name_id_guard' ORDER BY name;
DROP TABLE t_ids_name_id_guard SYNC;


-- ===== empty covering parts (DETACH/DROP PART, DROP PARTITION, TRUNCATE) must be id-keyed =====
-- Before the fix, createEmptyPart wrote the covering part's columns.txt from the current logical
-- schema without stamping ids, so system.parts_columns.column_id reported the logical name (e.g.
-- 'c1') instead of the stable id ('1'). Large old_parts_lifetime keeps the transient Outdated
-- covering part observable so the assertion is not racing GC.

-- DETACH PART
DROP TABLE IF EXISTS t_detach SYNC;
CREATE TABLE t_detach (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_detach VALUES (1);
ALTER TABLE t_detach ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_detach (a, c) VALUES (2, 20);
ALTER TABLE t_detach RENAME COLUMN c TO c1;
ALTER TABLE t_detach DETACH PART 'all_2_2_0';
SELECT 'detach_part', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_detach' AND column = 'c1';

-- DROP PART
DROP TABLE IF EXISTS t_drop SYNC;
CREATE TABLE t_drop (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_drop VALUES (1);
ALTER TABLE t_drop ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_drop (a, c) VALUES (2, 20);
ALTER TABLE t_drop RENAME COLUMN c TO c1;
ALTER TABLE t_drop DROP PART 'all_2_2_0';
SELECT 'drop_part', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_drop' AND column = 'c1';

-- DROP PARTITION
DROP TABLE IF EXISTS t_droppart SYNC;
CREATE TABLE t_droppart (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_droppart VALUES (1);
ALTER TABLE t_droppart ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_droppart (a, c) VALUES (2, 20);
ALTER TABLE t_droppart RENAME COLUMN c TO c1;
ALTER TABLE t_droppart DROP PARTITION tuple();
SELECT 'drop_partition', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_droppart' AND column = 'c1';

-- TRUNCATE
DROP TABLE IF EXISTS t_trunc SYNC;
CREATE TABLE t_trunc (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             old_parts_lifetime = 100000, merge_tree_clear_old_parts_interval_seconds = 100000;
INSERT INTO t_trunc VALUES (1);
ALTER TABLE t_trunc ADD COLUMN c UInt64 DEFAULT 7;
INSERT INTO t_trunc (a, c) VALUES (2, 20);
ALTER TABLE t_trunc RENAME COLUMN c TO c1;
TRUNCATE TABLE t_trunc;
SELECT 'truncate', arraySort(groupArrayDistinct(column_id))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_trunc' AND column = 'c1';

DROP TABLE t_detach SYNC;
DROP TABLE t_drop SYNC;
DROP TABLE t_droppart SYNC;
DROP TABLE t_trunc SYNC;


-- ===== CLEAR COLUMN must reset a renamed column to its default =====
-- The part keeps its load-time name after a metadata-only RENAME, so a name-resolved file drop
-- missed the id-keyed file, leaving the data intact (a silent no-op). The fix resolves the part's
-- column by its physical id. Covered for an added (id-keyed) column and an original (id == name).
SET mutations_sync = 2;
DROP TABLE IF EXISTS cv SYNC;
CREATE TABLE cv (k UInt64) ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0;
ALTER TABLE cv ADD COLUMN c UInt64 DEFAULT 0;
INSERT INTO cv VALUES (1, 111);
ALTER TABLE cv RENAME COLUMN c TO c1;
ALTER TABLE cv CLEAR COLUMN c1 IN PARTITION 1;
SELECT 'clear_added_renamed', c1 FROM cv;

DROP TABLE IF EXISTS co SYNC;
CREATE TABLE co (k UInt64, c UInt64) ENGINE = MergeTree PARTITION BY k ORDER BY k
    SETTINGS allow_experimental_column_ids = 1, serialization_info_version = 'with_column_ids',
             min_bytes_for_wide_part = 0;
INSERT INTO co VALUES (1, 111);
ALTER TABLE co RENAME COLUMN c TO c1;
ALTER TABLE co CLEAR COLUMN c1 IN PARTITION 1;
SELECT 'clear_original_renamed', c1 FROM co;
DROP TABLE cv SYNC;
DROP TABLE co SYNC;


-- ===== rename onto a name freed by an earlier rename/drop: orphan stream keeps its own slot =====
-- On reload the part's physical column list must resolve every key in ID-space: the live column
-- keeps the name, the orphan keeps its slot under its own key -- never re-bound to the live
-- sibling's stream. A regression here made the reloaded part a duplicate-column error.

-- Two-column rotation is rejected: after a -> x, column x holds id 'a', so renaming b onto 'a'
-- would make a logical name equal to another active column's id. It must fail loudly.
DROP TABLE IF EXISTS t_rot;
CREATE TABLE t_rot (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rot VALUES (1, 10, 20);
ALTER TABLE t_rot MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_rot RENAME COLUMN a TO x;
ALTER TABLE t_rot RENAME COLUMN b TO a; -- { serverError BAD_ARGUMENTS }

-- DROP then RENAME the survivor onto the dropped name -- Wide and Compact, both the last-slot
-- orphan (DROP b) and the middle-slot orphan (DROP a).
DROP TABLE IF EXISTS t_dropb_wide;
CREATE TABLE t_dropb_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dropb_wide VALUES (1, 10, 20);
ALTER TABLE t_dropb_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropb_wide DROP COLUMN b;
ALTER TABLE t_dropb_wide RENAME COLUMN a TO b;
DETACH TABLE t_dropb_wide;
ATTACH TABLE t_dropb_wide;
SELECT 'dropb_wide', k, b FROM t_dropb_wide;

DROP TABLE IF EXISTS t_dropa_wide;
CREATE TABLE t_dropa_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dropa_wide VALUES (1, 10, 20);
ALTER TABLE t_dropa_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropa_wide DROP COLUMN a;
ALTER TABLE t_dropa_wide RENAME COLUMN b TO a;
DETACH TABLE t_dropa_wide;
ATTACH TABLE t_dropa_wide;
SELECT 'dropa_wide', k, a FROM t_dropa_wide;

DROP TABLE IF EXISTS t_dropb_compact;
CREATE TABLE t_dropb_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dropb_compact VALUES (1, 10, 20);
ALTER TABLE t_dropb_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropb_compact DROP COLUMN b;
ALTER TABLE t_dropb_compact RENAME COLUMN a TO b;
DETACH TABLE t_dropb_compact;
ATTACH TABLE t_dropb_compact;
SELECT 'dropb_compact', k, b FROM t_dropb_compact;

DROP TABLE IF EXISTS t_dropa_compact;
CREATE TABLE t_dropa_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_dropa_compact VALUES (1, 10, 20);
ALTER TABLE t_dropa_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_dropa_compact DROP COLUMN a;
ALTER TABLE t_dropa_compact RENAME COLUMN b TO a;
DETACH TABLE t_dropa_compact;
ATTACH TABLE t_dropa_compact;
SELECT 'dropa_compact', k, a FROM t_dropa_compact;

-- DROP then re-ADD the same name: the reused name gets a fresh ID absent from the old part, so the
-- orphan is not re-adopted and the reader default-fills.
DROP TABLE IF EXISTS t_readd;
CREATE TABLE t_readd (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_readd VALUES (1, 99);
ALTER TABLE t_readd MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd DROP COLUMN a;
ALTER TABLE t_readd ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd;
ATTACH TABLE t_readd;
SELECT 'readd', k, a FROM t_readd;
DROP TABLE t_rot;
DROP TABLE t_dropb_wide;
DROP TABLE t_dropa_wide;
DROP TABLE t_dropb_compact;
DROP TABLE t_dropa_compact;
DROP TABLE t_readd;


-- ===== DROP + re-ADD: marks introspection must resolve the part column by id, not name =====
-- The re-added column gets a fresh id absent from the old part, whose original orphan stream still
-- sits on disk under the old id. The marks path must resolve by id, else it binds the orphan
-- stream and reports its marks for a column absent from the part. Only the marks path is observable.
DROP TABLE IF EXISTS t_readd_marks_wide;
CREATE TABLE t_readd_marks_wide (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_readd_marks_wide VALUES (1, 99);
ALTER TABLE t_readd_marks_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_marks_wide DROP COLUMN a;
ALTER TABLE t_readd_marks_wide ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd_marks_wide;
ATTACH TABLE t_readd_marks_wide;
SELECT 'wide_data', k, a FROM t_readd_marks_wide;
SELECT 'wide_readd_marks_null',
       min(tupleElement(`a.mark`, 1) IS NULL AND tupleElement(`a.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_readd_marks_wide', with_marks = true)
WHERE part_name = 'all_1_1_0';

DROP TABLE IF EXISTS t_readd_marks_compact;
CREATE TABLE t_readd_marks_compact (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_readd_marks_compact VALUES (1, 99);
ALTER TABLE t_readd_marks_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_readd_marks_compact DROP COLUMN a;
ALTER TABLE t_readd_marks_compact ADD COLUMN a UInt64 DEFAULT 7;
DETACH TABLE t_readd_marks_compact;
ATTACH TABLE t_readd_marks_compact;
SELECT 'compact_data', k, a FROM t_readd_marks_compact;
SELECT 'compact_readd_marks_null',
       min(tupleElement(`a.mark`, 1) IS NULL AND tupleElement(`a.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_readd_marks_compact', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_readd_marks_wide;
DROP TABLE t_readd_marks_compact;


-- ===== orphan placeholder collision: a fresh-id column absent from the part must default-fill =====
-- DROP b + RENAME a TO b makes the orphan's stale name 'b' collide with the live 'b'; the orphan is
-- renamed to a unique placeholder so the part's column list has no duplicate name. A later 'b_' is a
-- real column absent from the old part -- its marks must be NULL (fillMarks resolves by stable id).
DROP TABLE IF EXISTS t_orphan_collide_wide;
CREATE TABLE t_orphan_collide_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_orphan_collide_wide VALUES (1, 10, 20);
ALTER TABLE t_orphan_collide_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_orphan_collide_wide DROP COLUMN b;
ALTER TABLE t_orphan_collide_wide RENAME COLUMN a TO b;
ALTER TABLE t_orphan_collide_wide ADD COLUMN `b_` UInt64 DEFAULT 7;
DETACH TABLE t_orphan_collide_wide;
ATTACH TABLE t_orphan_collide_wide;
SELECT 'wide_data', k, b, `b_` FROM t_orphan_collide_wide;
SELECT 'wide_absent_marks_null',
       min(tupleElement(`b_.mark`, 1) IS NULL AND tupleElement(`b_.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_orphan_collide_wide', with_marks = true)
WHERE part_name = 'all_1_1_0';

DROP TABLE IF EXISTS t_orphan_collide_compact;
CREATE TABLE t_orphan_collide_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, index_granularity = 1;
INSERT INTO t_orphan_collide_compact VALUES (1, 10, 20);
ALTER TABLE t_orphan_collide_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE t_orphan_collide_compact DROP COLUMN b;
ALTER TABLE t_orphan_collide_compact RENAME COLUMN a TO b;
ALTER TABLE t_orphan_collide_compact ADD COLUMN `b_` UInt64 DEFAULT 7;
DETACH TABLE t_orphan_collide_compact;
ATTACH TABLE t_orphan_collide_compact;
SELECT 'compact_data', k, b, `b_` FROM t_orphan_collide_compact;
SELECT 'compact_absent_marks_null',
       min(tupleElement(`b_.mark`, 1) IS NULL AND tupleElement(`b_.mark`, 2) IS NULL) AS marks_null
FROM mergeTreeIndex(currentDatabase(), 't_orphan_collide_compact', with_marks = true)
WHERE part_name = 'all_1_1_0';
DROP TABLE t_orphan_collide_wide;
DROP TABLE t_orphan_collide_compact;


-- ===== minmax partition pruning must resolve each part's minmax file by the partition-key id =====
-- MinMaxIndex::load and the checkConsistencyBase minmax checks resolve id-first, so a foreign
-- column's minmax_<id>.idx can never bind to a different partition-key column. Mapping churn
-- (ADD/DROP of a non-partition column) keeps a non-trivial live mapping across a part reload.
DROP TABLE IF EXISTS mm_wide;
CREATE TABLE mm_wide (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO mm_wide VALUES (1, 10, 100), (2, 10, 100), (3, 20, 200), (4, 30, 300);
ALTER TABLE mm_wide MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE mm_wide ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE mm_wide DROP COLUMN c;
ALTER TABLE mm_wide ADD COLUMN d UInt64 DEFAULT 0;
INSERT INTO mm_wide VALUES (5, 10, 100, 9), (6, 40, 400, 9);
OPTIMIZE TABLE mm_wide FINAL;
DETACH TABLE mm_wide;
ATTACH TABLE mm_wide;
SELECT 'wide_a10', k, a, b, d FROM mm_wide WHERE a = 10 ORDER BY k;
SELECT 'wide_b300', k FROM mm_wide WHERE b = 300 ORDER BY k;
SELECT 'wide_a40_b400', k FROM mm_wide WHERE a = 40 AND b = 400 ORDER BY k;
SELECT 'wide_total', count() FROM mm_wide;

DROP TABLE IF EXISTS mm_compact;
CREATE TABLE mm_compact (k UInt64, a UInt64, b UInt64) ENGINE = MergeTree PARTITION BY (a, b) ORDER BY k
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO mm_compact VALUES (1, 10, 100), (2, 10, 100), (3, 20, 200), (4, 30, 300);
ALTER TABLE mm_compact MODIFY SETTING serialization_info_version = 'with_column_ids';
ALTER TABLE mm_compact ADD COLUMN c UInt64 DEFAULT 0;
ALTER TABLE mm_compact DROP COLUMN c;
ALTER TABLE mm_compact ADD COLUMN d UInt64 DEFAULT 0;
INSERT INTO mm_compact VALUES (5, 10, 100, 9), (6, 40, 400, 9);
OPTIMIZE TABLE mm_compact FINAL;
DETACH TABLE mm_compact;
ATTACH TABLE mm_compact;
SELECT 'compact_a10', k, a, b, d FROM mm_compact WHERE a = 10 ORDER BY k;
SELECT 'compact_b300', k FROM mm_compact WHERE b = 300 ORDER BY k;
SELECT 'compact_a40_b400', k FROM mm_compact WHERE a = 40 AND b = 400 ORDER BY k;
SELECT 'compact_total', count() FROM mm_compact;
DROP TABLE mm_wide;
DROP TABLE mm_compact;


-- ===== orphan column-size attribution: an orphan whose id-token equals a live name must be skipped =====
-- DROP a; ADD a: the re-added 'a' gets a fresh id, and the old part's original 'a' stream becomes an
-- orphan whose stamped id equals the new live column's logical name 'a'. The column-size aggregate
-- keys live columns by name and orphans by id-token (both plain strings that can coincide), so an
-- orphan with no live logical name must be skipped, else its bytes are attributed to the live 'a'.
DROP TABLE IF EXISTS t_orphan_size;
CREATE TABLE t_orphan_size (k UInt64, a String) ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_orphan_size SELECT number, toString(rand64()) || toString(rand64()) FROM numbers(20000);
SELECT 'dropped_a_has_bytes', data_compressed_bytes > 0 AS big
FROM system.columns WHERE database = currentDatabase() AND table = 't_orphan_size' AND name = 'a';
ALTER TABLE t_orphan_size DROP COLUMN a;
ALTER TABLE t_orphan_size ADD COLUMN a String;
DETACH TABLE t_orphan_size;
ATTACH TABLE t_orphan_size;
SELECT 'live_a_size_zero', data_compressed_bytes = 0 AS is_zero
FROM system.columns WHERE database = currentDatabase() AND table = 't_orphan_size' AND name = 'a';
DROP TABLE t_orphan_size;
