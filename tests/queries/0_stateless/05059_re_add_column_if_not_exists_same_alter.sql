-- Regression: a same-statement `DROP COLUMN x, ADD COLUMN IF NOT EXISTS x ...` must re-add the
-- column. `AlterCommands::prepare` judged the ADD's `IF NOT EXISTS` against the untouched original
-- schema (where x is still present), marked the ADD as a no-op, and the DROP silently removed x for
-- good. When x was the only column the table was left with an empty column list, so a later ALTER
-- raised "Cannot alter table ... metadata doesn't have structure".

DROP TABLE IF EXISTS re_add_ine;
CREATE TABLE re_add_ine (a Int64, x Int64, pad Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine VALUES (1, 10, 100), (2, 20, 200);

-- Dropping x and re-adding it (with IF NOT EXISTS) in one statement must leave x present, holding
-- the new default. Other columns keep their rows.
ALTER TABLE re_add_ine (DROP COLUMN x), (ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 7);
SELECT 're-add count', count(), min(a), max(a) FROM re_add_ine;
SELECT 're-add x', groupArray(x) FROM re_add_ine;

-- The table must still have a usable column list (subsequent reads and ALTERs work).
SELECT 'read after', a, x FROM re_add_ine ORDER BY a;

-- A duplicate ADD on a column that still exists (not dropped in this statement) is still a no-op.
ALTER TABLE re_add_ine ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 99;
SELECT 'dup-noop count', count() FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine' AND name = 'x';

DROP TABLE re_add_ine;

-- Nested: a plain `DROP COLUMN n` un-exists the whole flattened `n.*` range (ColumnsDescription::remove
-- walks the name prefix), while prepare() only saw the literal name `n`. The ADD of a flattened child
-- was wrongly skipped as a no-op against the original schema, silently losing `n.x`.
DROP TABLE IF EXISTS re_add_ine_nested;
CREATE TABLE re_add_ine_nested (a Int64, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_nested VALUES (1, [10], [20]), (2, [11], [21]);

ALTER TABLE re_add_ine_nested (DROP COLUMN n), (ADD COLUMN IF NOT EXISTS `n.x` Array(Int64));
SELECT 'nested re-add columns', name FROM system.columns
    WHERE database = currentDatabase() AND table = 're_add_ine_nested' ORDER BY name;
SELECT 'nested re-add data', a, n.x FROM re_add_ine_nested ORDER BY a;

DROP TABLE re_add_ine_nested;

-- Rename: after `RENAME COLUMN x TO x_old` the name x is free by apply time, so a same-statement
-- `ADD COLUMN IF NOT EXISTS x` is a genuine re-add and must not be skipped.
DROP TABLE IF EXISTS re_add_ine_rename;
CREATE TABLE re_add_ine_rename (x Int64, pad Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO re_add_ine_rename VALUES (42, 1);

ALTER TABLE re_add_ine_rename (RENAME COLUMN x TO x_old), (ADD COLUMN IF NOT EXISTS x Int64 DEFAULT 7);
SELECT 'rename re-add', x_old, x, pad FROM re_add_ine_rename ORDER BY x_old;

DROP TABLE re_add_ine_rename;
