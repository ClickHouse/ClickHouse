-- While an `ALTER TABLE ... RENAME COLUMN` of a Nested leaf was pending, reads of the renamed leaf
-- from a wide part returned default values: the reader asks for the leaf as a subcolumn of its parent
-- (`n.z` as the subcolumn `z` of `n`), while the rename is recorded under the flattened name `n.z`, so
-- the part was searched for a column that exists there only under the old name. The array sizes came
-- from the shared offsets stream, which made the result look well-formed.

DROP TABLE IF EXISTS t_pending_rename_nested;
CREATE TABLE t_pending_rename_nested (id UInt8, `n.a` Array(UInt8), `n.b` Array(String))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_pending_rename_nested VALUES (1, [7, 8], ['x', 'y']);

-- Stands in for a mutation that is still running on a big table.
SYSTEM STOP MERGES t_pending_rename_nested;
ALTER TABLE t_pending_rename_nested RENAME COLUMN `n.b` TO `n.z` SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'while the rename is pending', id, `n.a`, `n.z` FROM t_pending_rename_nested;
SELECT 'and through ARRAY JOIN', countIf(z != '') FROM t_pending_rename_nested ARRAY JOIN `n.z` AS z;
SELECT 'sizes are unaffected', length(`n.z`) FROM t_pending_rename_nested;

SYSTEM START MERGES t_pending_rename_nested;
ALTER TABLE t_pending_rename_nested DELETE WHERE 0 SETTINGS mutations_sync = 2;

SELECT 'after it materialized', id, `n.a`, `n.z` FROM t_pending_rename_nested;

SELECT 'a leaf name may itself contain dots';
-- `n.b.c` is the leaf `b.c` of `n`, and the rename may change any part of the leaf name, so the leaf
-- must be looked up in the rename map with its full name and not only with its first segment.
DROP TABLE IF EXISTS t_pending_rename_dotted;
CREATE TABLE t_pending_rename_dotted (id UInt8, `n.a` Array(UInt8), `n.b.c` Array(String), `n.b` Array(UInt8))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_pending_rename_dotted VALUES (1, [7, 8], ['x', 'y'], [3, 4]);
SYSTEM STOP MERGES t_pending_rename_dotted;
ALTER TABLE t_pending_rename_dotted RENAME COLUMN `n.b.c` TO `n.x.y` SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'while the rename is pending', id, `n.a`, `n.x.y`, `n.x.y.size0`, `n.b` FROM t_pending_rename_dotted;
SYSTEM START MERGES t_pending_rename_dotted;
ALTER TABLE t_pending_rename_dotted DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', id, `n.a`, `n.x.y`, `n.x.y.size0`, `n.b` FROM t_pending_rename_dotted;
DROP TABLE t_pending_rename_dotted;

SELECT 'an unchanged longer leaf next to a renamed shorter one';
-- `n.x.y` is an unchanged leaf, and it must not be taken for the subcolumn `y` of the leaf `n.x`,
-- which is the new name of `n.b` while the rename is pending.
DROP TABLE IF EXISTS t_pending_rename_prefix;
CREATE TABLE t_pending_rename_prefix (id UInt8, `n.x.y` Array(String), `n.b` Array(UInt8))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_pending_rename_prefix VALUES (1, ['p', 'q'], [3, 4]);
SYSTEM STOP MERGES t_pending_rename_prefix;
ALTER TABLE t_pending_rename_prefix RENAME COLUMN `n.b` TO `n.x` SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'while the rename is pending', id, `n.x.y`, `n.x.y.size0`, `n.x` FROM t_pending_rename_prefix;
SYSTEM START MERGES t_pending_rename_prefix;
ALTER TABLE t_pending_rename_prefix DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', id, `n.x.y`, `n.x.y.size0`, `n.x` FROM t_pending_rename_prefix;
DROP TABLE t_pending_rename_prefix;

SELECT 'a subcolumn of the renamed leaf';
DROP TABLE IF EXISTS t_pending_rename_subcolumn;
CREATE TABLE t_pending_rename_subcolumn (id UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_pending_rename_subcolumn VALUES (1, [7, 8], [NULL, 5]);
SYSTEM STOP MERGES t_pending_rename_subcolumn;
ALTER TABLE t_pending_rename_subcolumn RENAME COLUMN `n.b` TO `n.x` SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT 'while the rename is pending', id, `n.x`, `n.x.null`, `n.x.size0` FROM t_pending_rename_subcolumn;
SYSTEM START MERGES t_pending_rename_subcolumn;
ALTER TABLE t_pending_rename_subcolumn DELETE WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'after it materialized', id, `n.x`, `n.x.null`, `n.x.size0` FROM t_pending_rename_subcolumn;
DROP TABLE t_pending_rename_subcolumn;

SELECT 'a pending rename of an ordinary column is unaffected';
DROP TABLE IF EXISTS t_pending_rename_plain;
CREATE TABLE t_pending_rename_plain (id UInt8, v String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_pending_rename_plain VALUES (1, 'hello');
SYSTEM STOP MERGES t_pending_rename_plain;
ALTER TABLE t_pending_rename_plain RENAME COLUMN v TO w SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT id, w FROM t_pending_rename_plain;
SYSTEM START MERGES t_pending_rename_plain;
ALTER TABLE t_pending_rename_plain DELETE WHERE 0 SETTINGS mutations_sync = 2;

DROP TABLE t_pending_rename_plain;
DROP TABLE t_pending_rename_nested;
