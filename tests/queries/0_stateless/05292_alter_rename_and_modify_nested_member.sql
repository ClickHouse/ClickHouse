-- A single ALTER cannot rename a Nested member and modify it, as for an ordinary column.

DROP TABLE IF EXISTS t_rename_modify_nested;

CREATE TABLE t_rename_modify_nested (x UInt32, n Nested(a UInt32, y LowCardinality(String)))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_rename_modify_nested VALUES (1, [10, 20], ['a', 'bb']), (2, [30], ['ccc']);

ALTER TABLE t_rename_modify_nested RENAME COLUMN `n.y` TO `n.z`, MODIFY COLUMN `n.z` Array(String); -- { serverError NOT_IMPLEMENTED }
ALTER TABLE t_rename_modify_nested RENAME COLUMN `n.y` TO `n.z`, MODIFY COLUMN IF EXISTS `n.z` Array(String); -- { serverError NOT_IMPLEMENTED }

SELECT x, n.y FROM t_rename_modify_nested ORDER BY x;

-- Renaming one member and modifying another one in the same ALTER, then the type change in its own ALTER.
ALTER TABLE t_rename_modify_nested RENAME COLUMN `n.y` TO `n.z`, MODIFY COLUMN `n.a` Array(UInt64);
ALTER TABLE t_rename_modify_nested MODIFY COLUMN `n.z` Array(String);

SELECT x, n.a, n.z, toTypeName(n.a), toTypeName(n.z) FROM t_rename_modify_nested ORDER BY x;

DROP TABLE t_rename_modify_nested;

DROP TABLE IF EXISTS t_rename_modify_dotted;
CREATE TABLE t_rename_modify_dotted (x UInt32, `c0.c1` Array(UInt32)) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_rename_modify_dotted RENAME COLUMN `c0.c1` TO `c0.c2`, MODIFY COLUMN `c0.c2` Array(UInt64); -- { serverError NOT_IMPLEMENTED }
DROP TABLE t_rename_modify_dotted;
