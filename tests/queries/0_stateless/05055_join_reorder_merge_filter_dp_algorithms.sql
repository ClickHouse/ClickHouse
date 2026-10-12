-- Filters merged into the join graph (`query_plan_merge_filters_into_join`) become single-relation
-- predicates whose relation id can be >= 2 after flattening nested joins. The DP-based join order
-- algorithms (`dpsize`, `dpsub`) must attach such predicates at the leaf join of their relation and
-- must not silently drop them (which returned rows that should have been filtered out).

SET enable_analyzer = 1;
SET join_use_nulls = 0;
SET query_plan_optimize_join_order_limit = 64;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_expression_into_join = 1;
SET query_plan_merge_filters_into_join = 1;

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tc;
DROP TABLE IF EXISTS td;

CREATE TABLE ta (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tc (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE td (id Int32, x Int32) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO ta VALUES (1, 10), (2, 20), (3, 30), (5, 50), (6, 60);
INSERT INTO tc VALUES (1, 11), (2, 22), (3, 33), (5, 55), (6, 66);
INSERT INTO td VALUES (1, 1), (2, 222), (3, 3), (5, -5);

-- Filter on the null-supplying side of a LEFT JOIN cannot be pushed down below the join
-- (unmatched rows produce the default value 0, which passes `td.x < 100`), so it is merged
-- into the flattened join graph as a single-relation predicate of relation 2.
SELECT '-- filter merged from LEFT JOIN subquery --';

SELECT ta.id, r.dx FROM ta JOIN (SELECT tc.id AS id, td.x AS dx FROM tc LEFT JOIN td ON tc.id = td.id WHERE td.x < 100) r ON ta.id = r.id
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT ta.id, r.dx FROM ta JOIN (SELECT tc.id AS id, td.x AS dx FROM tc LEFT JOIN td ON tc.id = td.id WHERE td.x < 100) r ON ta.id = r.id
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT '-- filter merged from INNER JOIN subquery --';

SELECT ta.id, r.dx FROM ta JOIN (SELECT tc.id AS id, td.x AS dx FROM tc JOIN td ON tc.id = td.id WHERE td.x > 0) r ON ta.id = r.id
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', query_plan_filter_push_down = 0;

SELECT ta.id, r.dx FROM ta JOIN (SELECT tc.id AS id, td.x AS dx FROM tc JOIN td ON tc.id = td.id WHERE td.x > 0) r ON ta.id = r.id
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', query_plan_filter_push_down = 0;

SELECT ta.id, r.dx FROM ta JOIN (SELECT tc.id AS id, td.x AS dx FROM tc JOIN td ON tc.id = td.id WHERE td.x > 0) r ON ta.id = r.id
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_filter_push_down = 0;

-- A pair of relations connected only by a non-equi predicate must still produce a valid plan
-- in `dpsize` (a cross join with the predicate as a condition), not an exception.
SELECT '-- non-equi only join --';

SELECT ta.id, td.id FROM ta JOIN td ON ta.x < td.x
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT ta.id, td.id FROM ta JOIN td ON ta.x < td.x
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize';

SELECT '-- relation linked to the rest of the graph only by a non-equi predicate --';

SELECT ta.id, tc.id, td.id FROM ta JOIN tc ON ta.id = tc.id JOIN td ON td.x < ta.x
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT ta.id, tc.id, td.id FROM ta JOIN tc ON ta.id = tc.id JOIN td ON td.x < ta.x
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize';

DROP TABLE ta;
DROP TABLE tc;
DROP TABLE td;
