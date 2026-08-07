-- Merged filters must be split into conjunction atoms (including alias-wrapped and nested `and`s),
-- so that each predicate becomes a separate join-graph edge. A composite `and(...)` edge would
-- carry the union of all source relations and hide the underlying binary predicates from the
-- join-order solvers, shrinking the search space or making the graph look disconnected.

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

-- All conjuncts must be absorbed into the join graph: the plan has no Filter step left above the
-- joins, and both equality predicates are usable as join conditions (2 logical joins, 3 reads).
SELECT
    countIf(trimLeft(explain) LIKE 'ReadFromMergeTree%') AS reads,
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS boundary_filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, description = 0
    SELECT ta.id, ta.x, tb.y, tc.z
    FROM ta, tb, tc
    WHERE (ta.id = tb.id AND tb.id = tc.id) AND (ta.x > 0 AND tb.y < 400)
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_runtime_filters = 0);

-- The filter column may be an alias over `and(...)` (e.g. a condition defined in a subquery).
-- The alias must be unwrapped before splitting; otherwise the whole conjunction becomes a single
-- composite edge referencing all three relations and the equalities cannot drive the join order.
SELECT '-- alias-wrapped conjunction --';
SELECT id, z FROM (SELECT ta.id AS id, tc.z AS z, (ta.id = tb.id AND tb.id = tc.id AND ta.x > 0) AS cond FROM ta, tb, tc) WHERE cond
ORDER BY ALL;

-- Both joins must get an equality condition (INNER), no cross join and no leftover Filter step.
SELECT
    countIf(trimLeft(explain) LIKE 'JoinLogical%') AS joins,
    countIf(trimLeft(explain) LIKE 'Type: INNER%') AS inner_joins,
    countIf(trimLeft(explain) LIKE 'Type: CROSS%') AS cross_joins,
    countIf(trimLeft(explain) LIKE 'Filter%') AS boundary_filters
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
    SELECT id, z FROM (SELECT ta.id AS id, tc.z AS z, (ta.id = tb.id AND tb.id = tc.id AND ta.x > 0) AS cond FROM ta, tb, tc) WHERE cond
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_runtime_filters = 0);

DROP TABLE ta;
DROP TABLE tb;
DROP TABLE tc;
