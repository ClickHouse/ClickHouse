-- Regression: same-statement `DROP COLUMN x, ADD COLUMN IF NOT EXISTS x ...` must re-add the
-- column. `AlterCommands::prepare` judged the ADD's `IF NOT EXISTS` against the untouched original
-- schema, marked the ADD as a no-op, and the DROP silently removed x for good. The working schema
-- must advance per command (drop/rename un-exists, add re-exists), and the wide-part mutation fast
-- path must drop the files of the removed column under whatever name the part stores them.

DROP TABLE IF EXISTS re_add_ine;
CREATE TABLE re_add_ine (a Int64, x Int64, pad Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine VALUES (1, 10, 100), (2, 20, 200);

-- Dropping x and re-adding it in one statement leaves x present, holding the new default.
ALTER TABLE re_add_ine (DROP COLUMN x), (ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 7);
SELECT 're-add count', count(), min(a), max(a) FROM re_add_ine;
SELECT 're-add x', groupArray(x) FROM re_add_ine;
SELECT 'read after', a, x FROM re_add_ine ORDER BY a;

-- A duplicate ADD on a column that still exists is still a no-op.
ALTER TABLE re_add_ine ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 99;
SELECT 'dup-noop count', count() FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine' AND name = 'x';

DROP TABLE re_add_ine;

-- Nested: a plain `DROP COLUMN n` un-exists the whole flattened `n.*` range, so re-adding a
-- flattened child is a genuine re-add, not a no-op.
DROP TABLE IF EXISTS re_add_ine_nested;
CREATE TABLE re_add_ine_nested (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_nested VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE re_add_ine_nested (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 'nested re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine_nested' ORDER BY name;
SELECT 'nested re-add data', a, n.x FROM re_add_ine_nested ORDER BY a;

DROP TABLE re_add_ine_nested;

-- Mirror shapes: dropping the only child un-exists the group, so re-adding the group is a genuine
-- re-add; re-adding one child next to a surviving sibling reads defaults sized by shared offsets.
DROP TABLE IF EXISTS re_add_ine_nested_mirror;
CREATE TABLE re_add_ine_nested_mirror (a Int64, n Nested(x Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_nested_mirror VALUES (1, [10]), (2, [11]);

ALTER TABLE re_add_ine_nested_mirror (DROP COLUMN n.x), (ADD COLUMN IF NOT EXISTS n Nested(x Int64));
SELECT 'mirror re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine_nested_mirror' ORDER BY name;
SELECT 'mirror re-add data', a, n.x FROM re_add_ine_nested_mirror ORDER BY a;

DROP TABLE re_add_ine_nested_mirror;

DROP TABLE IF EXISTS re_add_ine_nested_mirror_two;
CREATE TABLE re_add_ine_nested_mirror_two (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_nested_mirror_two VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE re_add_ine_nested_mirror_two (DROP COLUMN n.x), (DROP COLUMN n.y), (ADD COLUMN IF NOT EXISTS n Nested(x Int64, y Int64));
SELECT 'mirror two re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine_nested_mirror_two' ORDER BY name;
SELECT 'mirror two re-add data', a, n.x, n.y FROM re_add_ine_nested_mirror_two ORDER BY a;

DROP TABLE re_add_ine_nested_mirror_two;

DROP TABLE IF EXISTS re_add_ine_nested_partial;
CREATE TABLE re_add_ine_nested_partial (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_nested_partial VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE re_add_ine_nested_partial (DROP COLUMN n.x), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 'partial re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine_nested_partial' ORDER BY name;
SELECT 'partial re-add data', a, n.x, n.y FROM re_add_ine_nested_partial ORDER BY a;

DROP TABLE re_add_ine_nested_partial;

-- After `RENAME COLUMN x TO x_old` the name x is free by apply time, so a same-statement re-add
-- must not be skipped.
DROP TABLE IF EXISTS re_add_ine_rename;
CREATE TABLE re_add_ine_rename (x Int64, pad Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_rename VALUES (42, 1);

ALTER TABLE re_add_ine_rename (RENAME COLUMN x TO x_old), (ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 7);
SELECT 'rename re-add', x_old, x, pad FROM re_add_ine_rename ORDER BY x_old;

DROP TABLE re_add_ine_rename;

-- The conditional form `DROP COLUMN IF EXISTS n` must recognize a Nested parent stored as
-- flattened members; apply's exact-only skip guard let the re-add run against unmodified metadata.
DROP TABLE IF EXISTS re_add_if_exists_nested;
CREATE TABLE re_add_if_exists_nested (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_if_exists_nested VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE re_add_if_exists_nested (DROP COLUMN IF EXISTS n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 'if-exists nested re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_if_exists_nested' ORDER BY name;
SELECT 'if-exists nested re-add data', a, n.x FROM re_add_if_exists_nested ORDER BY a;

DROP TABLE re_add_if_exists_nested;

-- The skip still fires for a genuinely absent name.
DROP TABLE IF EXISTS drop_if_exists_missing;
CREATE TABLE drop_if_exists_missing (a Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO drop_if_exists_missing VALUES (1);

ALTER TABLE drop_if_exists_missing (DROP COLUMN IF EXISTS b), (DROP COLUMN IF EXISTS n);
SELECT 'missing skip columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_if_exists_missing' ORDER BY name;
SELECT 'missing skip data', a FROM drop_if_exists_missing ORDER BY a;

DROP TABLE drop_if_exists_missing;

-- Wide parts (hardlink fast path): dropping a flattened Nested parent must remove the member
-- files, otherwise they are hardlinked into the new part and a same-name re-add reads stale data.
DROP TABLE IF EXISTS drop_nested_wide;
CREATE TABLE drop_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO drop_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);

SELECT 'part type', part_type FROM system.parts
    WHERE database = currentDatabase() AND table = 'drop_nested_wide' AND active;

ALTER TABLE drop_nested_wide DROP COLUMN n;
SELECT 'after drop parent', a FROM drop_nested_wide ORDER BY a;
SELECT 'columns after drop parent', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'drop_nested_wide' ORDER BY name;
CHECK TABLE drop_nested_wide;
DROP TABLE drop_nested_wide;

CREATE TABLE readd_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO readd_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);
ALTER TABLE readd_nested_wide (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 're-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 'readd_nested_wide' ORDER BY name;
SELECT 're-add data', a, n.x FROM readd_nested_wide ORDER BY a;
CHECK TABLE readd_nested_wide;
DROP TABLE readd_nested_wide;

CREATE TABLE clear_nested_wide (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_wide VALUES (1, [10], [20]), (2, [11], [21]);
ALTER TABLE clear_nested_wide CLEAR COLUMN n;
SELECT 'after clear parent', a, n.x, n.y FROM clear_nested_wide ORDER BY a;
CHECK TABLE clear_nested_wide;
DROP TABLE clear_nested_wide;

-- A DROP/CLEAR after a rename (same mutation or pending) must drop the files under the name the
-- part stores, not the current name -- otherwise the rename carries the old data forward.
DROP TABLE IF EXISTS clear_nested_after_rename;
CREATE TABLE clear_nested_after_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO clear_nested_after_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE clear_nested_after_rename (RENAME COLUMN n.x TO n.z), (CLEAR COLUMN n)
SETTINGS mutations_sync = 2;

SELECT 'rename and clear in one alter', a, n.z, n.y FROM clear_nested_after_rename ORDER BY a;
CHECK TABLE clear_nested_after_rename;
DROP TABLE clear_nested_after_rename;

CREATE TABLE clear_nested_after_pending_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_after_pending_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE clear_nested_after_pending_rename RENAME COLUMN n.x TO n.z SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_pending_rename CLEAR COLUMN n SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_pending_rename UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;

SELECT 'clear after pending rename', a, n.z, n.y FROM clear_nested_after_pending_rename ORDER BY a;
CHECK TABLE clear_nested_after_pending_rename;
DROP TABLE clear_nested_after_pending_rename;

CREATE TABLE clear_nested_after_applied_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO clear_nested_after_applied_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE clear_nested_after_applied_rename RENAME COLUMN n.x TO n.z SETTINGS alter_sync = 0, mutations_sync = 0;
ALTER TABLE clear_nested_after_applied_rename UPDATE a = a WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE clear_nested_after_applied_rename CLEAR COLUMN n SETTINGS mutations_sync = 2;

SELECT 'clear after applied rename', a, n.z, n.y FROM clear_nested_after_applied_rename ORDER BY a;
CHECK TABLE clear_nested_after_applied_rename;
DROP TABLE clear_nested_after_applied_rename;

CREATE TABLE drop_nested_after_rename (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO drop_nested_after_rename VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE drop_nested_after_rename (RENAME COLUMN n.x TO n.z), (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64))
SETTINGS mutations_sync = 2;

SELECT 'drop and re-add after rename', a, n.x FROM drop_nested_after_rename ORDER BY a;
CHECK TABLE drop_nested_after_rename;
DROP TABLE drop_nested_after_rename;

-- The same carry-forward for a scalar column renamed in the same mutation.
DROP TABLE IF EXISTS readd_scalar_after_rename;
CREATE TABLE readd_scalar_after_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO readd_scalar_after_rename VALUES (10, 1);

ALTER TABLE readd_scalar_after_rename (RENAME COLUMN a TO b), (DROP COLUMN b), (ADD COLUMN IF NOT EXISTS b UInt64 DEFAULT 7);
SELECT 'scalar drop and re-add after rename', k, b FROM readd_scalar_after_rename;
CHECK TABLE readd_scalar_after_rename;
DROP TABLE readd_scalar_after_rename;

CREATE TABLE clear_scalar_after_rename (a UInt64, k UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO clear_scalar_after_rename VALUES (10, 1);

ALTER TABLE clear_scalar_after_rename (RENAME COLUMN a TO b), (CLEAR COLUMN b);
SELECT 'scalar rename and clear', k, b FROM clear_scalar_after_rename;
CHECK TABLE clear_scalar_after_rename;
DROP TABLE clear_scalar_after_rename;

-- Implicit minmax indices follow the column: a prefix-range DROP removes the indices of all
-- flattened members, and a RENAME replaces a stale implicit index at the target name. Nested
-- members never get implicit indices, but a scalar dotted-name column does.
DROP TABLE IF EXISTS t_implicit_orphan;
CREATE TABLE t_implicit_orphan (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_implicit_orphan VALUES (1, [10], [20]);

SELECT 'nested members have no implicit index', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

ALTER TABLE t_implicit_orphan DROP COLUMN n;
ALTER TABLE t_implicit_orphan ADD COLUMN `n.x` Int64;
SELECT 'scalar column got an index', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

ALTER TABLE t_implicit_orphan RENAME COLUMN `n.x` TO `n.z`;
SELECT 'the index follows the rename', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

ALTER TABLE t_implicit_orphan DROP COLUMN n;
SELECT 'no orphan after dropping the parent', name FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;

ALTER TABLE t_implicit_orphan (ADD COLUMN IF NOT EXISTS `n.x` Int64), (RENAME COLUMN `n.x` TO `n.z`)
    SETTINGS mutations_sync = 2;
SELECT 're-add and rename in one alter', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_orphan' ORDER BY name;
SELECT a, n.z FROM t_implicit_orphan ORDER BY a;
CHECK TABLE t_implicit_orphan;
DROP TABLE t_implicit_orphan;

CREATE TABLE t_implicit_clear (a Int64, n Nested(x Int64, y Int64))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS add_minmax_index_for_numeric_columns = 1;

INSERT INTO t_implicit_clear VALUES (1, [10], [20]);

ALTER TABLE t_implicit_clear DROP COLUMN n;
ALTER TABLE t_implicit_clear ADD COLUMN `n.x` Int64;
ALTER TABLE t_implicit_clear ADD COLUMN `n.w` Int64;

-- `CLEAR COLUMN n` keeps the column definitions, so the implicit indices stay.
ALTER TABLE t_implicit_clear (CLEAR COLUMN n) SETTINGS mutations_sync = 2;
SELECT 'clear keeps the indices', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_clear' ORDER BY name;

ALTER TABLE t_implicit_clear (ADD COLUMN IF NOT EXISTS `n.x` Int64), (RENAME COLUMN `n.x` TO `n.z`)
    SETTINGS mutations_sync = 2;
SELECT 'rename replaces the stale index', name, expr, creation FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_implicit_clear' ORDER BY name;
SELECT a, n.z, n.w FROM t_implicit_clear ORDER BY a;
CHECK TABLE t_implicit_clear;
DROP TABLE t_implicit_clear;

-- The snapshot advances with the full column definition, so properties of a just-added column
-- can be removed by a later command of the same ALTER.
DROP TABLE IF EXISTS re_add_full_desc;
CREATE TABLE re_add_full_desc (k UInt64) ENGINE = MergeTree ORDER BY k;

ALTER TABLE re_add_full_desc
    (ADD COLUMN x UInt64 DEFAULT 7 COMMENT 'c' CODEC(ZSTD)),
    (MODIFY COLUMN x REMOVE DEFAULT),
    (MODIFY COLUMN x REMOVE COMMENT),
    (MODIFY COLUMN x REMOVE CODEC);
SELECT name, default_expression, comment, compression_codec FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_full_desc' AND name = 'x';

DROP TABLE re_add_full_desc;
