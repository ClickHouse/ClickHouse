DROP TABLE IF EXISTS t_alias_subcolumn_comment;

CREATE TABLE t_alias_subcolumn_comment (id UInt64, parr Array(UInt64), arr Array(UInt64) ALIAS [id, id])
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_alias_subcolumn_comment VALUES (7, [1, 2, 3]);

SELECT arr.size0, parr.size0 FROM t_alias_subcolumn_comment;

ALTER TABLE t_alias_subcolumn_comment COMMENT COLUMN arr 'a', COMMENT COLUMN parr 'p';

SELECT arr.size0, parr.size0 FROM t_alias_subcolumn_comment;

SELECT name, comment FROM system.columns
WHERE database = currentDatabase() AND table = 't_alias_subcolumn_comment' AND comment != '' ORDER BY name;

ALTER TABLE t_alias_subcolumn_comment MODIFY COLUMN arr REMOVE ALIAS;

-- The part predates the physical column, so this reads the type default.
SELECT arr.size0 FROM t_alias_subcolumn_comment;

DROP TABLE t_alias_subcolumn_comment;
