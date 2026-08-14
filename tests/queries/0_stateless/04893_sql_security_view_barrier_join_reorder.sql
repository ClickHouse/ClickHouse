-- A `SQL SECURITY DEFINER` / `NONE` view that can hide rows is an optimization barrier: its whole
-- subplan is sealed by a step carrying the flag. The join-order optimizer used to peel that seal in
-- `addChildQueryGraph` (either as a trivial pass-through step, or by merging it into the child join
-- when `query_plan_merge_expression_into_join = 1`, the default) and then flatten the view's own join
-- into the invoker's join graph, so the invoker's joins were reordered across the view boundary.

-- Join reordering is a feature of the analyzer only.
SET enable_analyzer = 1;

-- Pin everything the plan shape depends on: the two passes involved, a deterministic relation order,
-- and no extra steps from runtime filters or distributed planning.
SET query_plan_merge_expression_into_join = 1, query_plan_optimize_join_order_limit = 64,
    query_plan_optimize_join_order_randomize = 0, enable_join_runtime_filters = 0,
    enable_parallel_replicas = 0, make_distributed_plan = 0;

DROP TABLE IF EXISTS big04893;
DROP TABLE IF EXISTS small04893;
DROP TABLE IF EXISTS outer04893;

-- Every column is named differently: the join graph is not flattened at all when the relations have
-- overlapping column names, which would make the oracle below non-discriminating.
CREATE TABLE big04893 (kb UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO big04893 SELECT number, number FROM numbers(100000);
CREATE TABLE small04893 (ks UInt64, w UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO small04893 SELECT number, number FROM numbers(10);
CREATE TABLE outer04893 (ko UInt64, u UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO outer04893 SELECT number, number FROM numbers(5);

-- The inner query of the view contains a join and a `WHERE`, so it can hide rows and gets the flag.
CREATE VIEW v04893_invoker SQL SECURITY INVOKER AS
    SELECT kb, v, w FROM big04893 INNER JOIN small04893 ON big04893.kb = small04893.ks WHERE v != 3;
CREATE VIEW v04893_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS
    SELECT kb, v, w FROM big04893 INNER JOIN small04893 ON big04893.kb = small04893.ks WHERE v != 3;

-- The `INVOKER` twin is flattened into the join graph of the invoker: its seal is merged into the
-- view's own join and the three relations are reordered by their estimated sizes, so no step
-- converting the result of the view subquery is left in the plan. This is the positive control
-- proving that the oracle discriminates.
SELECT 'invoker view is flattened into the join graph:', countIf(explain LIKE '%Convert VIEW subquery result%') = 0
FROM (EXPLAIN compact = 0 SELECT count() FROM v04893_invoker INNER JOIN outer04893 ON v04893_invoker.kb = outer04893.ko);

-- The `DEFINER` twin is sealed: the seal is neither peeled nor merged into the view's join, so the
-- view stays one opaque relation of the join graph of the invoker.
SELECT 'definer view stays sealed:', countIf(explain LIKE '%Convert VIEW subquery result%') = 1
FROM (EXPLAIN compact = 0 SELECT count() FROM v04893_definer INNER JOIN outer04893 ON v04893_definer.kb = outer04893.ko);

-- The barrier drops the optimization, never the result: the hidden row (`v = 3`) stays hidden and
-- both twins agree.
SELECT 'results:',
    (SELECT count() FROM v04893_definer INNER JOIN outer04893 ON v04893_definer.kb = outer04893.ko),
    (SELECT count() FROM v04893_invoker INNER JOIN outer04893 ON v04893_invoker.kb = outer04893.ko);

DROP VIEW v04893_invoker;
DROP VIEW v04893_definer;
DROP TABLE big04893;
DROP TABLE small04893;
DROP TABLE outer04893;
