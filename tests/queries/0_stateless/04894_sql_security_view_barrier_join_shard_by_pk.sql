-- Tags: long

-- `optimizeJoinByShards` (setting `query_plan_join_shard_by_pk_ranges`) walks from a join down to the
-- reading steps of both sides, merging the expression and filter DAGs it crosses on the way, and then
-- makes both readings emit one output port per primary-key range so that the join runs range by range.
-- The walk used to cross the seal of a `SQL SECURITY DEFINER` / `NONE` view that can hide rows, which
-- let the join keys of the invoker decide which rows the reading inside the view produces, in which
-- order, and on which port.

-- Join sharding by primary-key ranges is a feature of the analyzer only.
SET enable_analyzer = 1;

-- Pin everything the optimization depends on: it fires for the sorting merge join, needs several
-- threads, needs the reading to be in primary-key order, and the legacy `EXPLAIN` format prints the
-- `Sharding` line. `optimize_read_in_order` is randomized by the test harness, and with it off the
-- positive controls below stop sharding.
SET optimize_read_in_order = 1,
    query_plan_join_shard_by_pk_ranges = 1, join_algorithm = 'full_sorting_merge', max_threads = 4,
    query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_statistics = 0, enable_join_runtime_filters = 0, enable_parallel_replicas = 0,
    make_distributed_plan = 0, explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS l04894;
DROP TABLE IF EXISTS r04894;

CREATE TABLE l04894 (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO l04894 SELECT number, number FROM numbers(1000000);
CREATE TABLE r04894 (k UInt32, w UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO r04894 SELECT number, number FROM numbers(1000000);

-- A projection-only view hides nothing, so it never becomes a barrier and keeps the optimization.
CREATE VIEW v04894_projection DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, v FROM l04894;

-- A view with a `WHERE` can hide rows and is sealed.
CREATE VIEW v04894_definer DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT k, v FROM l04894 WHERE v != 3;
CREATE VIEW v04894_invoker SQL SECURITY INVOKER AS SELECT k, v FROM l04894 WHERE v != 3;

-- Positive controls: the sharding is applied for plain tables, for a view that hides nothing, and for
-- the `INVOKER` twin of the filtering view. They prove that the oracle below discriminates.
SELECT 'plain tables are sharded:', countIf(explain LIKE '%Sharding: %') = 1
FROM (EXPLAIN actions = 1 SELECT * FROM l04894 INNER JOIN r04894 ON l04894.k = r04894.k);

SELECT 'a view that hides nothing is sharded:', countIf(explain LIKE '%Sharding: %') = 1
FROM (EXPLAIN actions = 1 SELECT * FROM v04894_projection INNER JOIN r04894 ON v04894_projection.k = r04894.k);

SELECT 'the invoker twin is sharded:', countIf(explain LIKE '%Sharding: %') = 1
FROM (EXPLAIN actions = 1 SELECT * FROM v04894_invoker INNER JOIN r04894 ON v04894_invoker.k = r04894.k);

-- The barrier: nothing is derived through the seal of the filtering `DEFINER` view, so its reading
-- keeps producing the rows the view chose, not the ranges the join of the invoker asked for.
SELECT 'the sealed view is not sharded:', countIf(explain LIKE '%Sharding: %') = 0
FROM (EXPLAIN actions = 1 SELECT * FROM v04894_definer INNER JOIN r04894 ON v04894_definer.k = r04894.k);

-- The barrier drops the optimization, never the result.
SELECT 'results:',
    (SELECT count() FROM v04894_definer INNER JOIN r04894 ON v04894_definer.k = r04894.k),
    (SELECT count() FROM v04894_invoker INNER JOIN r04894 ON v04894_invoker.k = r04894.k);

DROP VIEW v04894_projection;
DROP VIEW v04894_definer;
DROP VIEW v04894_invoker;
DROP TABLE l04894;
DROP TABLE r04894;
