DROP TABLE IF EXISTS test;

-- A plain key column
CREATE TABLE test (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE test DROP COLUMN IF EXISTS a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN b;
DROP TABLE test;

-- Columns used inside key expressions, not as keys themselves
CREATE TABLE test (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree PARTITION BY intDiv(a, 10) ORDER BY (b + 1);
ALTER TABLE test DROP COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN c;
DROP TABLE test;

-- Only a subcolumn of the column is used in the key
CREATE TABLE test (a Tuple(x UInt64, y UInt64), b UInt64) ENGINE = MergeTree ORDER BY a.x;
ALTER TABLE test CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN b;
DROP TABLE test;

-- Dropping the common prefix of Nested columns drops every column of the group, so a key column
-- inside the group forbids the drop. Without a key column inside, the drop of the group is allowed
CREATE TABLE test (`n.a` UInt64, `n.b` UInt64, x UInt64) ENGINE = MergeTree ORDER BY `n.a`;
ALTER TABLE test DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN IF EXISTS n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE test;
CREATE TABLE test (`n.a` UInt64, `n.b` UInt64, x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test DROP COLUMN n;
ALTER TABLE test ADD COLUMN `n.a` UInt64, ADD COLUMN `n.b` UInt64;
-- The IF EXISTS form has to drop the group from the metadata too, not only its data
ALTER TABLE test DROP COLUMN IF EXISTS n;
ALTER TABLE test DROP COLUMN n; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
DROP TABLE test;

-- Dropping the group is rejected when a DEFAULT of another column reads a column of the group
CREATE TABLE test (`n.a` UInt64, x UInt64, c UInt64 DEFAULT `n.a` + 1) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test DROP COLUMN n; -- { serverError ILLEGAL_COLUMN }
ALTER TABLE test DROP COLUMN IF EXISTS n; -- { serverError ILLEGAL_COLUMN }
DROP TABLE test;

-- Dropping the group is rejected while a materialized view reads a column of the group
CREATE TABLE test (`n.a` UInt64, `n.b` UInt64, x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW test_mv ENGINE = Null AS SELECT `n.a` FROM test;
ALTER TABLE test DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN IF EXISTS n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE test_mv;
ALTER TABLE test DROP COLUMN n;
DROP TABLE test;

-- Dropping the group is rejected while an unfinished mutation touches a column of the group
CREATE TABLE test (`n.a` UInt64, x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO test VALUES (1, 1);
SYSTEM STOP MERGES test;
ALTER TABLE test UPDATE `n.a` = 2 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE test DROP COLUMN n; -- { serverError BAD_ARGUMENTS }
ALTER TABLE test DROP COLUMN IF EXISTS n; -- { serverError BAD_ARGUMENTS }
KILL MUTATION WHERE database = currentDatabase() AND table = 'test' SYNC FORMAT Null;
SYSTEM START MERGES test;
ALTER TABLE test DROP COLUMN n;
DROP TABLE test;

-- Without shared Nested offsets the name does not denote the group: it is an unknown name to drop,
-- and a no-op under IF EXISTS
CREATE TABLE test (`n.a` UInt64, x UInt64) ENGINE = MergeTree ORDER BY `n.a` SETTINGS share_nested_offsets = 0;
ALTER TABLE test DROP COLUMN n; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
ALTER TABLE test DROP COLUMN IF EXISTS n;
DROP TABLE test;

-- A special column of the engine that is a key column as well
CREATE TABLE test (a UInt64, s Int8, b UInt64) ENGINE = CollapsingMergeTree(s) ORDER BY (a, s);
ALTER TABLE test DROP COLUMN s; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN s; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN b;
DROP TABLE test;

-- The column is checked before the partition expression, so a key column is rejected even when the
-- partition is malformed. For other columns the partition is still validated first
CREATE TABLE test (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree PARTITION BY b ORDER BY a;
ALTER TABLE test CLEAR COLUMN a IN PARTITION 'nonsense'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN c IN PARTITION 'nonsense'; -- { serverError TYPE_MISMATCH }
ALTER TABLE test CLEAR COLUMN c IN PARTITION 1;
DROP TABLE test;

-- A column can be put into the key by the same ALTER that drops it. It is not a key column yet, so
-- the drop is not rejected up front and the error comes from the recalculation of the key
CREATE TABLE test (a UInt64) ENGINE = MergeTree ORDER BY a;
ALTER TABLE test ADD COLUMN b UInt64, MODIFY ORDER BY (a, b), DROP COLUMN b; -- { serverError UNKNOWN_IDENTIFIER }
ALTER TABLE test ADD COLUMN b UInt64, MODIFY ORDER BY (a, b);
ALTER TABLE test DROP COLUMN b; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE test;

-- A column used only in a TTL expression is not a key column, so the drop is allowed together with
-- a rewrite of the TTL. The column the new expression refers to is protected in turn
CREATE TABLE test (d Date, e Date, a UInt64) ENGINE = MergeTree ORDER BY a TTL d + INTERVAL 1 DAY;
ALTER TABLE test DROP COLUMN d, MODIFY TTL e + INTERVAL 1 DAY;
ALTER TABLE test DROP COLUMN e; -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE test;
