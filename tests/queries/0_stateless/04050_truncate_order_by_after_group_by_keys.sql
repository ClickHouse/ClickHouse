-- Tags: no-fasttest
-- When there is GROUP BY and ORDER BY, trailing ORDER BY elements after
-- all GROUP BY keys are covered are removed as an optimization.
-- This also applies when GROUP BY ALL and ORDER BY ALL are used together.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a String, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t VALUES ('x', 1, 10), ('y', 2, 20), ('x', 3, 30), ('y', 4, 40);

-- ORDER BY a, sum(b): key `a` covered at position 1 → sum(b) removed
SELECT a, sum(b) FROM t GROUP BY a ORDER BY a, sum(b);

-- ORDER BY sum(b), a, sum(c): key `a` covered at position 2 → sum(c) removed
SELECT sum(b), a, sum(c) FROM t GROUP BY a ORDER BY sum(b), a, sum(c);

-- Multiple GROUP BY keys: ORDER BY a, b, sum(c) → sum(c) removed after both keys covered
SELECT a, b, sum(c) FROM t GROUP BY a, b ORDER BY a, b, sum(c);

-- No GROUP BY keys covered in ORDER BY → no truncation
SELECT sum(b), sum(c) FROM t GROUP BY a ORDER BY sum(b), sum(c);

-- Injective function: toString(a) covers GROUP BY key `a` → sum(b) removed
SELECT toString(a), sum(b) FROM t GROUP BY a ORDER BY toString(a), sum(b);

-- GROUP BY ALL + ORDER BY ALL: sum(b), a, sum(c) → ORDER BY sum(b), a
SELECT sum(b), a, sum(c) FROM t GROUP BY ALL ORDER BY ALL;

-- GROUP BY ALL + ORDER BY ALL: a, sum(b) → ORDER BY a
SELECT a, sum(b) FROM t GROUP BY ALL ORDER BY ALL;

-- GROUP BY ALL + ORDER BY ALL: no GROUP BY keys → no truncation
SELECT sum(b), sum(c) FROM t GROUP BY ALL ORDER BY ALL;

-- Verify DESC is preserved
SELECT a, sum(b) FROM t GROUP BY a ORDER BY a DESC, sum(b) DESC;

-- Verify that EXPLAIN QUERY TREE shows only 2 SORT nodes (sum(c) was removed)
SELECT count()
FROM (EXPLAIN QUERY TREE SELECT sum(b), a, sum(c) FROM t GROUP BY a ORDER BY sum(b), a, sum(c))
WHERE explain LIKE '%SORT id:%';

-- COLLATE: distinct GROUP BY values may compare equal under the collation,
-- so tiebreakers must be preserved — no truncation should happen.
SELECT count()
FROM (EXPLAIN QUERY TREE SELECT a, sum(b) FROM t GROUP BY a ORDER BY a COLLATE 'en_US', sum(b))
WHERE explain LIKE '%SORT id:%';

DROP TABLE t;
