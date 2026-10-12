-- A `Filter` step merged into the join graph contributes its conjunction atoms as separate
-- join-graph edges (aliases unwrapped, nested `and`s split), instead of one composite `and(...)`
-- edge carrying the union of all source relations, so the filter no longer terminates the join
-- chain. The queries below cover alias-wrapped and nested conjunctions; the plan check at the end
-- pins the absorption.

SET enable_analyzer = 1;
SET query_plan_merge_filters_into_join = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tb;
DROP TABLE IF EXISTS tc;

CREATE TABLE ta (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tb (id Int32, y Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tc (id Int32, z Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO ta VALUES (1, 10), (2, -20), (3, 30), (4, 40);
INSERT INTO tb VALUES (1, 100), (2, 200), (3, 300), (5, 500);
INSERT INTO tc VALUES (1, 1000), (3, 3000), (4, 4000), (6, 6000);

SELECT '-- comma join, conjunctive WHERE --';
SELECT ta.id, ta.x, tb.y, tc.z
FROM ta, tb, tc
WHERE ta.id = tb.id AND tb.id = tc.id AND ta.x > 0
ORDER BY ALL;

SELECT '-- nested conjunctions --';
SELECT ta.id, ta.x, tb.y, tc.z
FROM ta, tb, tc
WHERE (ta.id = tb.id AND tb.id = tc.id) AND (ta.x > 0 AND tb.y < 400)
ORDER BY ALL;

-- The filter column may be an alias over `and(...)` (e.g. a condition defined in a subquery).
SELECT '-- alias-wrapped conjunction --';
SELECT id, z FROM (SELECT ta.id AS id, tc.z AS z, (ta.id = tb.id AND tb.id = tc.id AND ta.x > 0) AS cond FROM ta, tb, tc) WHERE cond
ORDER BY ALL;

SELECT '-- alias-wrapped conjunction between two joins --';
SELECT u.id, u.v, tc.z
FROM (SELECT ta.id AS id, ta.x + tb.y AS v, (ta.x + tb.y > 50 AND ta.x + tb.id > 5) AS cond
      FROM ta JOIN tb ON ta.id = tb.id WHERE cond) u
JOIN tc ON u.id = tc.id
ORDER BY ALL;

-- Join reordering is the only optimization that can absorb the filter of the query above: it sits
-- between the two joins, every atom of its condition spans both `ta` and `tb` (so nothing can be
-- pushed down to a single table), and `query_plan_merge_filter_into_join_condition` is disabled.
-- With `query_plan_merge_filters_into_join` the atoms become join-graph edges and no `Filter` step
-- is left; without it the step stays and terminates the join chain. The remaining settings are
-- pinned because the assertion counts plan steps.
SELECT
    'merge on' AS merge,
    countIf(trimLeft(explain) LIKE 'ReadFromMergeTree%') AS reads,
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, description = 0
    SELECT u.id, u.v, tc.z
    FROM (SELECT ta.id AS id, ta.x + tb.y AS v, (ta.x + tb.y > 50 AND ta.x + tb.id > 5) AS cond
          FROM ta JOIN tb ON ta.id = tb.id WHERE cond) u
    JOIN tc ON u.id = tc.id
    SETTINGS query_plan_merge_filters_into_join = 1, query_plan_merge_filter_into_join_condition = 0,
             query_plan_merge_expression_into_join = 1, query_plan_merge_filters = 1,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 0);

SELECT
    'merge off' AS merge,
    countIf(trimLeft(explain) LIKE 'ReadFromMergeTree%') AS reads,
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, description = 0
    SELECT u.id, u.v, tc.z
    FROM (SELECT ta.id AS id, ta.x + tb.y AS v, (ta.x + tb.y > 50 AND ta.x + tb.id > 5) AS cond
          FROM ta JOIN tb ON ta.id = tb.id WHERE cond) u
    JOIN tc ON u.id = tc.id
    SETTINGS query_plan_merge_filters_into_join = 0, query_plan_merge_filter_into_join_condition = 0,
             query_plan_merge_expression_into_join = 1, query_plan_merge_filters = 1,
             query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 0);

DROP TABLE ta;
DROP TABLE tb;
DROP TABLE tc;
