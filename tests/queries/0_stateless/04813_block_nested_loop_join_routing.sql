-- Tags: no-old-analyzer

-- Routing of a `JOIN ON` condition that no other algorithm can claim: instead of failing with
-- `INVALID_JOIN_ON_EXPRESSION`, the plan gets a `BlockNestedLoopJoin` step. `INNER`/`CROSS` with
-- `ALL` strictness keep their cross-join-with-a-filter plan, so nothing that worked before moves
-- to the new operator.

SET join_algorithm = 'direct,parallel_hash,hash';
SET cross_to_inner_join_rewrite = 0;

DROP TABLE IF EXISTS bnl_l;
DROP TABLE IF EXISTS bnl_r;

CREATE TABLE bnl_l (id Int32, x Int32, y Int32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bnl_r (id Int32, x Int32, y Int32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO bnl_l VALUES (1, 1, 3, 'a'), (2, 2, 2, 'ab'), (3, 3, 1, 'b');
INSERT INTO bnl_r VALUES (1, 1, 1, 'a%'), (2, 2, 3, 'b%'), (3, 3, 2, '%');

-- Every outer kind and every non-`ALL` strictness with a single inequality: the new step.
SELECT 'left', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'right', count() FROM (EXPLAIN SELECT count() FROM bnl_l l RIGHT JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'full', count() FROM (EXPLAIN SELECT count() FROM bnl_l l FULL JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'left semi', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT SEMI JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'left anti', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'right semi', count() FROM (EXPLAIN SELECT count() FROM bnl_l l RIGHT SEMI JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'right anti', count() FROM (EXPLAIN SELECT count() FROM bnl_l l RIGHT ANTI JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'left any', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT ANY JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'inner any', count() FROM (EXPLAIN SELECT count() FROM bnl_l l INNER ANY JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- Condition shapes beyond a comparison of two columns.
SELECT 'function', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.s LIKE r.s) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'arithmetic', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x + l.y > r.x) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'one-sided', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.y > 1) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'not equals', count() FROM (EXPLAIN SELECT count() FROM bnl_l l FULL JOIN bnl_r r ON l.x != r.x) WHERE explain LIKE '%BlockNestedLoopJoin%';
-- A disjunction with a keyless disjunct: not claimable as hash clauses either.
SELECT 'disjunction', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x OR l.y < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- No takeover: `INNER`/`CROSS` with `ALL` strictness still cross join and filter.
SELECT 'inner', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'inner cross', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%Type: cross%';
SELECT 'inner filter', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%Filter (Post Join Actions)%';
SELECT 'cross', count() FROM (EXPLAIN SELECT count() FROM bnl_l l, bnl_r r WHERE l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- No takeover: an equality is still claimed as a hash join key, and a disjunction of
-- equi-clauses is still claimed as several hash clauses.
SELECT 'equi', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'equi mixed', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x AND l.y < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'equi disjunction', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x OR l.y = r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- No takeover: IEJoin still claims two inequalities when it is enabled.
SET join_algorithm = 'direct,parallel_hash,hash,ie_join';
SELECT 'ie join', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'ie join step', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';
SET join_algorithm = 'direct,parallel_hash,hash';

-- `ASOF` prescribes the shape of its condition, so an unusable condition still fails.
SELECT count() FROM bnl_l l ASOF JOIN bnl_r r ON l.s LIKE r.s; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM bnl_l l ASOF JOIN bnl_r r ON l.x < r.x AND l.y > r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- The operator answers one candidate pair per row of the batch it evaluates the condition on, so a
-- condition that changes the row count is rejected instead of being evaluated against the wrong pairs.
SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON arrayJoin([l.x < r.y, l.x > r.y]); -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM bnl_l l FULL JOIN bnl_r r ON arrayJoin([l.x < r.y]); -- { serverError INVALID_JOIN_ON_EXPRESSION }

DROP TABLE bnl_l;
DROP TABLE bnl_r;
