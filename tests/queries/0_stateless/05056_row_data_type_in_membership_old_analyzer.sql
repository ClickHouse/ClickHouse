-- Tags: no-fasttest

SET allow_experimental_row_type = 1;
SET enable_analyzer = 0;

DROP TABLE IF EXISTS row_in_old;
CREATE TABLE row_in_old (a UInt64, r Row(x UInt64, y String)) ENGINE = MergeTree ORDER BY a;
INSERT INTO row_in_old VALUES (1, (1, 'a')), (2, (1, 'b')), (3, (0, 'z'));

-- A tuple RHS is a set of row values, not a set of scalars.
SELECT a, r IN ((1, 'a')), r IN ((1, 'a'), (0, 'z')), r NOT IN ((1, 'a')) FROM row_in_old ORDER BY a;
SELECT a FROM row_in_old WHERE r IN ((1, 'a'), (1, 'b')) ORDER BY a;
SELECT a FROM row_in_old WHERE r NOT IN ((1, 'a'), (1, 'b')) ORDER BY a;

-- A non-constant tuple-valued RHS takes the row-wise rewrite instead of a constant Set.
SELECT a, r IN (materialize((1, 'a'))) FROM row_in_old ORDER BY a;
SELECT a, r NOT IN (materialize((1, 'a'))) FROM row_in_old ORDER BY a;

-- Subqueries on the right-hand side, producing tuple and Row values.
SELECT a, r IN (SELECT (1, 'a')) FROM row_in_old ORDER BY a;
SELECT a, r IN (SELECT r FROM row_in_old WHERE a != 2) FROM row_in_old ORDER BY a;
SELECT (1, 'b') IN (SELECT r FROM row_in_old);

DROP TABLE row_in_old;
