DROP TABLE IF EXISTS t_alias_subcolumn_comment;

CREATE TABLE t_alias_subcolumn_comment (id UInt64, parr Array(UInt64), arr Array(UInt64) ALIAS [id, id])
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_subcolumn_comment VALUES (7, [1, 2, 3]);

SELECT arr.size0, parr.size0 FROM t_alias_subcolumn_comment;

ALTER TABLE t_alias_subcolumn_comment COMMENT COLUMN arr 'a', COMMENT COLUMN parr 'p';

SELECT arr.size0, parr.size0 FROM t_alias_subcolumn_comment;

SELECT name, comment FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_subcolumn_comment' AND comment != '' ORDER BY name;

-- RENAME COLUMN re-applies the same code path to every column of the table, so renaming an
-- unrelated column must not break the alias subcolumn either.
ALTER TABLE t_alias_subcolumn_comment RENAME COLUMN parr TO parr2;

SELECT arr.size0, parr2.size0 FROM t_alias_subcolumn_comment;

ALTER TABLE t_alias_subcolumn_comment MODIFY COLUMN arr REMOVE ALIAS;

INSERT INTO t_alias_subcolumn_comment VALUES (8, [1, 2], [9, 9, 9, 9]);

-- The first row predates the physical column and reads the type default; the second holds data, so
-- a read that answered with a default for both would be visible here.
SELECT arr.size0 FROM t_alias_subcolumn_comment ORDER BY id;

-- A column that stops being physical must stop being registered as having static subcolumns.
ALTER TABLE t_alias_subcolumn_comment MODIFY COLUMN parr2 Array(UInt64) ALIAS [id, id];

SELECT parr2.size0 FROM t_alias_subcolumn_comment ORDER BY id;

DROP TABLE t_alias_subcolumn_comment;
