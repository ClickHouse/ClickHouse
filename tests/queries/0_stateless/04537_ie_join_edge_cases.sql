-- Tags: no-old-analyzer

-- IEJoin edge cases: empty, single-row and NULL-key inputs, boundary row positions,
-- block-size-aligned output, and shapes that must fall back to other join algorithms.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT number AS x, number AS y FROM numbers(5)) a
    JOIN (SELECT number AS x, number AS y FROM numbers(5)) b
    ON a.x < b.x AND a.y > b.y
) WHERE explain LIKE '%IEJoin%';

-- Empty right input
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(5)) a JOIN (SELECT number AS x, number AS y FROM numbers(0)) b ON a.x < b.x AND a.y > b.y;
-- Empty left input
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(0)) a JOIN (SELECT number AS x, number AS y FROM numbers(5)) b ON a.x < b.x AND a.y > b.y;
-- Both inputs empty
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(0)) a JOIN (SELECT number AS x, number AS y FROM numbers(0)) b ON a.x < b.x AND a.y > b.y;

-- Single-row inputs, match
SELECT a.x, a.y, b.x, b.y FROM (SELECT number AS x, number + 10 AS y FROM numbers(1)) a JOIN (SELECT number + 1 AS x, number + 5 AS y FROM numbers(1)) b ON a.x < b.x AND a.y > b.y;
-- Single-row inputs, no match
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(1)) a JOIN (SELECT number + 1 AS x, number + 5 AS y FROM numbers(1)) b ON a.x < b.x AND a.y > b.y;

-- A match involving the first row of both sides: the matching left entry sits at L1 position 0
-- and both matched rows have row id 1 (row ids are 1-based signed, positions are 0-based,
-- so this catches any mix-up between the two at the boundary).
DROP TABLE IF EXISTS pos0_l;
DROP TABLE IF EXISTS pos0_r;
CREATE TABLE pos0_l (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE pos0_r (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO pos0_l VALUES (0, 10), (5, 20);
INSERT INTO pos0_r VALUES (1, 5), (2, 20);
SELECT a.x, a.y, b.x, b.y FROM pos0_l a JOIN pos0_r b ON a.x < b.x AND a.y > b.y ORDER BY ALL;
DROP TABLE pos0_l;
DROP TABLE pos0_r;

-- Output size is an exact multiple of the block size: forces the resumable-return path
-- when a batch fills up exactly at the last result row
SELECT count() FROM (SELECT toInt64(number) AS x, toInt64(number) AS y FROM numbers(10)) a JOIN (SELECT toInt64(number + 100) AS x, toInt64(number) - 1 AS y FROM numbers(1)) b ON a.x < b.x AND a.y > b.y SETTINGS max_block_size = 5;
SELECT count() FROM (SELECT toInt64(number) AS x, toInt64(number) AS y FROM numbers(5)) a JOIN (SELECT toInt64(number + 100) AS x, toInt64(number) - 1 AS y FROM numbers(1)) b ON a.x < b.x AND a.y > b.y SETTINGS max_block_size = 5;
SELECT a.x FROM (SELECT toInt64(number) AS x, toInt64(number) AS y FROM numbers(10)) a JOIN (SELECT toInt64(number + 100) AS x, toInt64(number) - 1 AS y FROM numbers(1)) b ON a.x < b.x AND a.y > b.y ORDER BY a.x SETTINGS max_block_size = 5;

-- Rows with NULL in any key must be excluded from the result of an INNER join
DROP TABLE IF EXISTS tn1;
DROP TABLE IF EXISTS tn2;
CREATE TABLE tn1 (x Nullable(Int32), y Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tn2 (x Nullable(Int32), y Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tn1 VALUES (1, 10), (NULL, 10), (1, NULL), (NULL, NULL), (5, 50);
INSERT INTO tn2 VALUES (2, 5), (NULL, 5), (2, NULL), (10, 100);
SELECT a.x, a.y, b.x, b.y FROM tn1 a JOIN tn2 b ON a.x < b.x AND a.y > b.y ORDER BY ALL;
DROP TABLE tn1;
DROP TABLE tn2;

-- Anti-correlated conditions: empty result
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(100)) a JOIN (SELECT number AS x, number AS y FROM numbers(100)) b ON a.x < b.x AND a.y > b.y;

-- Non-overlapping ranges must produce an empty result with both orders of the conditions
-- (the second order exercises the short circuit on the second condition)
DROP TABLE IF EXISTS overlap_left;
DROP TABLE IF EXISTS overlap_right;
CREATE TABLE overlap_left (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE overlap_right (x Int32, y Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO overlap_left VALUES (1, 10), (2, 11), (3, 12), (4, 13);
INSERT INTO overlap_right VALUES (1, 101), (2, 102), (3, 103), (4, 104);
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT t1.x, t2.x FROM overlap_left t1 JOIN overlap_right t2 ON t1.x < t2.x AND t1.y > t2.y) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM overlap_left t1 JOIN overlap_right t2 ON t1.x < t2.x AND t1.y > t2.y;
-- Reverse condition order
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT t1.x, t2.x FROM overlap_left t1 JOIN overlap_right t2 ON t1.y > t2.y AND t1.x < t2.x) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM overlap_left t1 JOIN overlap_right t2 ON t1.y > t2.y AND t1.x < t2.x;
DROP TABLE overlap_left;
DROP TABLE overlap_right;

-- More than two inequality conditions: for INNER the first two become the IEJoin conditions
-- and the rest a filter over the join result
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count()
    FROM (SELECT number AS x, number AS y FROM numbers(3)) a
    JOIN (SELECT number AS x, number AS y FROM numbers(3)) b
    ON a.x < b.x AND a.y < b.y AND a.x + a.y < b.x + b.y
) WHERE explain LIKE '%IEJoin%';
SELECT count() FROM (SELECT number AS x, number AS y FROM numbers(3)) a JOIN (SELECT number AS x, number AS y FROM numbers(3)) b ON a.x < b.x AND a.y < b.y AND a.x + a.y < b.x + b.y;

-- A single inequality condition: not routed through IEJoin, still gives correct results
SELECT count() FROM (SELECT number AS x FROM numbers(3)) a JOIN (SELECT number AS x FROM numbers(3)) b ON a.x < b.x;
