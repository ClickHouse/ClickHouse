-- CHECK constraints may reference subcolumns (e.g. `.null` of a Nullable column, `.size0` of an Array).
-- Such a subcolumn is not physically present in the block that is checked on INSERT, so it has to be
-- extracted from its parent column. Regression test for issue #81911 (follow-up to #73689): previously
-- these constraints raised `NOT_FOUND_COLUMN_IN_BLOCK` on INSERT instead of being evaluated.

DROP TABLE IF EXISTS t_constraint_subcolumn;

-- Nullable `.null` subcolumn, MergeTree.
CREATE TABLE t_constraint_subcolumn (c0 Nullable(Int32), CONSTRAINT c CHECK c0.null = 0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_constraint_subcolumn VALUES (1), (2), (3);
INSERT INTO t_constraint_subcolumn VALUES (NULL); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_subcolumn;
DROP TABLE t_constraint_subcolumn;

-- Exact reproducer from the issue: ALTER ADD CONSTRAINT referencing a subcolumn, then INSERT.
-- `c0.null > 1` can never hold (the null map is 0 or 1), so the INSERT must be rejected as a
-- constraint violation rather than failing to find the column.
CREATE TABLE t_constraint_subcolumn (c0 Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_constraint_subcolumn ADD CONSTRAINT c CHECK c0.null > 1;
INSERT INTO t_constraint_subcolumn (c0) VALUES (1); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_subcolumn;
DROP TABLE t_constraint_subcolumn;

-- Array `.size0` subcolumn.
CREATE TABLE t_constraint_subcolumn (arr Array(Int32), CONSTRAINT c CHECK arr.size0 <= 3) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_constraint_subcolumn VALUES ([]), ([1]), ([1, 2, 3]);
INSERT INTO t_constraint_subcolumn VALUES ([1, 2, 3, 4]); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_subcolumn;
DROP TABLE t_constraint_subcolumn;

-- A constraint referencing both the parent column and its subcolumn.
CREATE TABLE t_constraint_subcolumn (c0 Nullable(Int32), CONSTRAINT c CHECK c0.null = 1 OR c0 > 0) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_constraint_subcolumn VALUES (10), (NULL), (20);
INSERT INTO t_constraint_subcolumn VALUES (-1); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_subcolumn;
DROP TABLE t_constraint_subcolumn;

-- Memory engine works the same way.
CREATE TABLE t_constraint_subcolumn (c0 Nullable(Int32), CONSTRAINT c CHECK c0.null = 0) ENGINE = Memory;
INSERT INTO t_constraint_subcolumn VALUES (5);
INSERT INTO t_constraint_subcolumn VALUES (NULL); -- { serverError VIOLATED_CONSTRAINT }
SELECT count() FROM t_constraint_subcolumn;
DROP TABLE t_constraint_subcolumn;
