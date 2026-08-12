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

-- Taken over: an outer kind with a single inequality, a non-`ALL` strictness, a condition that is
-- not a comparison of two columns, and a disjunction with a keyless disjunct.
SELECT 'left', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'left any', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT ANY JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'function', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.s LIKE r.s) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'disjunction', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x OR l.y < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';

-- No takeover: `INNER` with `ALL` strictness still cross joins and filters, an equality is still
-- claimed as a hash join key, and `IEJoin` still claims two inequalities when it is enabled.
SELECT 'inner', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'inner cross', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%Type: cross%';
SELECT 'inner filter', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%Filter (Post Join Actions)%';
SELECT 'equi', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x) WHERE explain LIKE '%BlockNestedLoopJoin%';
SELECT 'ie join', count() FROM (EXPLAIN SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.x AND l.y > r.y
    SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join') WHERE explain LIKE '%IEJoin%';

-- `EXPLAIN actions = 1` describes the operator: the kind, the strictness and the whole condition it
-- evaluates, named by its condition column.
SET explain_query_plan_default = 'legacy';
SELECT trimLeft(explain) FROM (
    EXPLAIN actions = 1 SELECT count() FROM bnl_l l LEFT ANTI JOIN bnl_r r ON l.x < r.y)
WHERE startsWith(trimLeft(explain), 'BlockNestedLoopJoin')
   OR startsWith(trimLeft(explain), 'Type: ')
   OR startsWith(trimLeft(explain), 'Strictness: ')
   OR startsWith(trimLeft(explain), 'Condition: ');
SET explain_query_plan_default = 'pretty';

-- `ASOF` prescribes the shape of its condition, so an unusable condition still fails.
SELECT count() FROM bnl_l l ASOF JOIN bnl_r r ON l.s LIKE r.s; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- The operator answers one candidate pair per row of the batch it evaluates the condition on, so a
-- condition that changes the row count is rejected instead of being evaluated against the wrong pairs.
SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON arrayJoin([l.x < r.y, l.x > r.y]); -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- `compatibility` with a version before the operator shipped rejects what reaches it, as that
-- version did. Checked before the setting is assigned explicitly, which would shadow it.
SET compatibility = '26.7';
SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SET compatibility = '';

-- The same switch on its own. Only the operator is off: the paths above it still claim what they
-- claimed.
SET allow_block_nested_loop_join = 0;
SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x OR l.y < r.y; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT 'off inner cross', count() FROM (EXPLAIN SELECT count() FROM bnl_l l JOIN bnl_r r ON l.x < r.y) WHERE explain LIKE '%Type: cross%';
SELECT 'off equi', count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x = r.x;

SET allow_block_nested_loop_join = 1;
SELECT 'on again', count() FROM bnl_l l LEFT JOIN bnl_r r ON l.x < r.y;

DROP TABLE bnl_l;
DROP TABLE bnl_r;
