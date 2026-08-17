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

-- The same when the subcolumn is used in the partition key
CREATE TABLE test (a Tuple(x UInt64, y UInt64), b UInt64) ENGINE = MergeTree PARTITION BY a.x ORDER BY b;
ALTER TABLE test DROP COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
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

-- With shared nested offsets a name that is not a column denotes the whole Nested group <name>.*,
-- and dropping or clearing the group is rejected when any column of the group is used in a key
CREATE TABLE test (`n.a` UInt64, `n.b` UInt64, x UInt64) ENGINE = MergeTree ORDER BY `n.a`;
ALTER TABLE test DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN IF EXISTS n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN `n.b`;
DROP TABLE test;

-- The same for a real Nested column and for a group column inside a key expression
CREATE TABLE test (n Nested(a UInt64, b UInt64), x UInt64) ENGINE = MergeTree PARTITION BY intDiv(`n.b`, 10) ORDER BY (x, `n.a`);
ALTER TABLE test DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE test;

-- The same when a column of the group is used in the key only through its subcolumn
CREATE TABLE test (`n.a` Tuple(x UInt64, y UInt64), `n.b` UInt64, z UInt64) ENGINE = MergeTree ORDER BY n.a.x;
ALTER TABLE test DROP COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test CLEAR COLUMN n; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE test DROP COLUMN `n.b`;
DROP TABLE test;

-- A group with no key columns inside can still be dropped
CREATE TABLE test (n Nested(a UInt64, b UInt64), x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE test DROP COLUMN n;
DROP TABLE test;

-- Without shared nested offsets the name does not denote the group and the check does not apply
CREATE TABLE test (`n.a` UInt64, x UInt64) ENGINE = MergeTree ORDER BY `n.a` SETTINGS share_nested_offsets = 0;
ALTER TABLE test DROP COLUMN n; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
DROP TABLE test;
