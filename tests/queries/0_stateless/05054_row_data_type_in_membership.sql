-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_in;
CREATE TABLE row_in (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_in VALUES (1, (1, 'a')), (2, (1, 'b')), (3, (0, 'z'));

-- A tuple RHS is a set of row values, not a set of scalars.
SELECT a, r IN ((1, 'a')), r IN ((1, 'a'), (0, 'z')), r NOT IN ((1, 'a')) FROM row_in ORDER BY a;
SELECT a FROM row_in WHERE r IN ((1, 'a'), (1, 'b')) ORDER BY a;
SELECT a FROM row_in WHERE r NOT IN ((1, 'a'), (1, 'b')) ORDER BY a;

-- Subqueries on the right-hand side, producing tuple and Row values.
SELECT a, r IN (SELECT (1, 'a')) FROM row_in ORDER BY a;
SELECT a, r IN (SELECT r FROM row_in WHERE a != 2) FROM row_in ORDER BY a;
SELECT (1, 'b') IN (SELECT r FROM row_in);

-- A Row column as a one-element set.
SELECT a, r IN r, (1, 'a') IN r FROM row_in ORDER BY a;

-- CAST from Row to the equivalent Tuple.
SELECT CAST((1, 'a')::Row(x UInt64, y String) AS Tuple(x UInt64, y String)) AS t, toTypeName(t);

DROP TABLE row_in;
