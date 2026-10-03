-- A mutation that hardlinks an untouched `Nested` member must record the type the part has for it,
-- even when the type in metadata is different because the `ALTER MODIFY COLUMN` mutation was killed.
-- Previously the part kept the type in metadata as the type in storage of the member, the member was lost
-- when the `Nested` columns of the part were collected, and reading it threw a logical error.

DROP TABLE IF EXISTS t_killed_modify_nested;

CREATE TABLE t_killed_modify_nested (x UInt32, n Nested(a Int8, y String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_killed_modify_nested VALUES (1, [10, 20], ['a', 'bb']), (2, [30, 40], ['ccc', 'dddd']);

SYSTEM STOP MERGES t_killed_modify_nested;
ALTER TABLE t_killed_modify_nested MODIFY COLUMN `n.a` QBit(Int8, 2) SETTINGS alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_killed_modify_nested' SYNC FORMAT Null;
SYSTEM START MERGES t_killed_modify_nested;

ALTER TABLE t_killed_modify_nested MODIFY COLUMN `n.y` Array(LowCardinality(String)) SETTINGS alter_sync = 1;

SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_killed_modify_nested' AND active AND NOT startsWith(column, '_')
ORDER BY column;

SELECT x, n.a, toTypeName(n.a), n.y FROM t_killed_modify_nested ORDER BY x;

DROP TABLE t_killed_modify_nested;
