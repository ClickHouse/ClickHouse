-- Tags: no-random-settings, no-random-merge-tree-settings
-- why: column-ID corner cases -- metadata-only RENAME/DROP mechanics, rejected ALTERs, TTL, partition transfer, projections, Nested.

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
INSERT INTO t_ids_multi_op VALUES (3, 'three', 2.72);
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
