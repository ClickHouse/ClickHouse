-- A `Filter` step that keeps its filter column (the column is also selected) and sits above an outer
-- join is merged into the join graph with `query_plan_merge_filters_into_join`. The kept column must
-- still be computed after the outer join, on the default-filled (or NULL-extended) values of the
-- unmatched rows, not before the join on the null-supplying side.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 0;
SET query_plan_convert_outer_join_to_inner_join = 0;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS ta;
DROP TABLE IF EXISTS tc;
DROP TABLE IF EXISTS td;

CREATE TABLE ta (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE tc (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE td (id UInt64, x UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO ta VALUES (1), (2);
INSERT INTO tc VALUES (1), (2);
INSERT INTO td VALUES (1, 200);

SELECT '-- LEFT JOIN, filter column selected';
SELECT cond FROM (SELECT tc.id, (td.x < 100) AS cond FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond) r
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0;

SELECT '-- LEFT JOIN below another join, filter column selected';
SELECT r.id, cond FROM ta JOIN (SELECT tc.id, (td.x < 100) AS cond FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0;

SELECT '-- LEFT JOIN below another join, expression over filter column selected';
SELECT r.id, c2 FROM ta JOIN (SELECT tc.id, (td.x < 100) AS cond, toUInt8(cond) + 1 AS c2 FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond AND tc.id > 0) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0;

SELECT '-- RIGHT JOIN below another join, filter column selected';
SELECT r.id, cond FROM ta JOIN (SELECT td.id AS id, (tc.id < 100) AS cond FROM tc RIGHT JOIN td ON tc.id = td.id + 5 WHERE cond) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0;

SELECT '-- join_use_nulls = 1, NULL-extended rows are filtered out';
SELECT r.id, cond FROM ta JOIN (SELECT tc.id, (td.x < 100) AS cond FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 1;

SELECT '-- all join order algorithms agree';
SELECT r.id, cond FROM ta JOIN (SELECT tc.id, (td.x < 100) AS cond FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0, query_plan_optimize_join_order_algorithm = 'dpsize,greedy';
SELECT r.id, cond FROM ta JOIN (SELECT tc.id, (td.x < 100) AS cond FROM tc LEFT JOIN td ON tc.id = td.id WHERE cond) r ON ta.id = r.id ORDER BY ALL
SETTINGS query_plan_merge_filters_into_join = 1, join_use_nulls = 0, query_plan_optimize_join_order_algorithm = 'dpsub,greedy';

DROP TABLE ta;
DROP TABLE tc;
DROP TABLE td;
