-- The Cascades optimizer must run only when `make_distributed_plan` is also
-- enabled (as documented in ARCHITECTURE.md). With `make_distributed_plan = 0`
-- the exchange steps it inserts are silently built as no-op pipeline steps, so
-- e.g. a partial aggregation created for `WITH TOTALS` reaches `TotalsHaving`
-- unmerged and produces duplicate groups.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_gating;

CREATE TABLE t_gating (k UInt64, x UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_gating SELECT number % 5, number FROM numbers(1000);

SELECT '-- 1. WITH TOTALS with cascades on but make_distributed_plan off: must be a plain single-node query';
SELECT k, sum(x) FROM t_gating GROUP BY k WITH TOTALS ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

SELECT '-- 2. Plain aggregation, same settings';
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

SELECT '-- 3. With both settings on, WITH TOTALS is rejected (fail-close)';
SELECT k, sum(x) FROM t_gating GROUP BY k WITH TOTALS ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- A LOCAL JOIN must use only co-located data; a distributed Cascades plan cannot
-- guarantee that, so it is rejected.
SELECT '-- 4. LOCAL JOIN is rejected (fail-close)';
DROP TABLE IF EXISTS t_gating_dim;
CREATE TABLE t_gating_dim (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_gating_dim SELECT number % 5 FROM numbers(10);
SELECT count() FROM t_gating AS a LOCAL INNER JOIN t_gating_dim AS b ON a.k = b.k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_gating AS a LOCAL INNER JOIN t_gating_dim AS b ON a.k = b.k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;
DROP TABLE t_gating_dim;

-- `force_aggregation_in_order` makes an in-order aggregation, which assumes its input arrives
-- ordered by the group keys - the exchanges Cascades inserts do not preserve that. The pre-check
-- rejects it cleanly instead of building a plan that would return wrong groups.
SELECT '-- 5. force_aggregation_in_order is rejected (in-order aggregation is not serializable)';
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    force_aggregation_in_order = 1, distributed_plan_execute_locally = 1; -- { serverError SUPPORT_IS_DISABLED }

-- The trivial-count rewrite is disabled under Cascades (its `ReadFromPreparedSource` leaf
-- cannot be cloned); the count runs as a distributed read instead.
SELECT '-- 6. Trivial count works under Cascades';
SELECT count() FROM t_gating
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- Reads without clone support (e.g. the `viewExplain` table function) are rejected up front.
SELECT '-- 7. A read without clone support is rejected (fail-close)';
SELECT count() FROM (EXPLAIN PLAN SELECT 1)
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- A distributed read is bucketed and cannot be served from a projection, so projections are
-- turned off under `make_distributed_plan`; a forced projection is ignored and the query still works.
SELECT '-- 8. A forced projection is ignored (still correct)';
DROP TABLE IF EXISTS t_gating_proj;
CREATE TABLE t_gating_proj (a UInt64, b UInt64, PROJECTION p_agg (SELECT b, sum(a) GROUP BY b))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_gating_proj SELECT number, number % 5 FROM numbers(1000);
SELECT b, sum(a) FROM t_gating_proj GROUP BY b ORDER BY b
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    distributed_plan_execute_locally = 1, optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_gating_proj;

-- A read from a `Distributed` table with remote shards fans out by itself and cannot be part
-- of the distributed plan; it is rejected up front. (With localhost shards the shard subplans
-- are inlined and planned locally, so the same query works.)
SELECT '-- 9. A read from remote shards is rejected (fail-close), localhost shards work';
DROP TABLE IF EXISTS t_gating_dist;
CREATE TABLE t_gating_dist AS t_gating ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_gating);
SELECT count() FROM t_gating_dist
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    prefer_localhost_replica = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_gating_dist
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    prefer_localhost_replica = 1, distributed_plan_execute_locally = 1;
DROP TABLE t_gating_dist;

-- `WITH FILL` is not supported yet; without `make_distributed_plan` the same query runs single-node.
SELECT '-- 10. WITH FILL is rejected (fail-close)';
SELECT k FROM t_gating WHERE k IN (0, 2, 4) GROUP BY k ORDER BY k WITH FILL
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT k FROM t_gating WHERE k IN (0, 2, 4) GROUP BY k ORDER BY k WITH FILL
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

-- A subquery `WITH TOTALS` sets "read till the end" on the outer limit, which the two-stage
-- top-N split cannot honor; such plans are rejected by the `WITH TOTALS` gate before any rule runs.
SELECT '-- 11. Subquery WITH TOTALS under an outer top-N is rejected (fail-close)';
SELECT k, s FROM (SELECT k, sum(x) AS s FROM t_gating GROUP BY k WITH TOTALS) ORDER BY s DESC LIMIT 3
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- The `STREAM` read rejection lives in `04670_cascades_streaming_queries_gating`: a test that
-- uses `enable_streaming_queries` must carry `_streaming_queries_` in its name (style check).

-- In-order aggregation relies on its input order, which the exchanges do not preserve: the
-- in-order rewrite is skipped under `make_distributed_plan` (hash aggregation runs instead),
-- and a planner-forced in-order aggregation is rejected up front on both planner paths.
SELECT '-- 12. optimize_aggregation_in_order works via hash aggregation, force_ is rejected (fail-close)';
-- Distributed aggregation cannot enforce a global `max_rows_to_group_by`, so pin it to 0.
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS optimize_aggregation_in_order = 1, make_distributed_plan = 1, enable_cascades_optimizer = 0,
    distributed_plan_execute_locally = 1, max_rows_to_group_by = 0;
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS force_aggregation_in_order = 1, make_distributed_plan = 1, enable_cascades_optimizer = 0,
    distributed_plan_execute_locally = 1, max_rows_to_group_by = 0; -- { serverError SUPPORT_IS_DISABLED }

-- A plan that receives no exchanges (the read stays below the broadcast threshold) but carries
-- a step without serialization support must run via the local fallback, not fail on the
-- fragment serializability check.
SELECT '-- 13. Exchange-free plan with a window falls back to local execution';
SELECT DISTINCT sum(x) OVER () FROM t_gating
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 0, distributed_plan_execute_locally = 1;

-- `join_any_take_last_row` pins which matching row an ANY join keeps: the last one built into
-- the hash table. A commutativity swap changes the build side, so it would change the result;
-- the swap alternative is not generated for such joins (the non-Cascades reordering suppresses
-- the swap the same way). `max_threads = 1` keeps the build order, and so the expected row,
-- deterministic.
SELECT '-- 14. ANY join with join_any_take_last_row is not swapped (must return the last row)';
DROP TABLE IF EXISTS t_gating_any;
DROP TABLE IF EXISTS t_gating_one;
CREATE TABLE t_gating_any (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY (k, v);
INSERT INTO t_gating_any SELECT 1, number + 2 FROM numbers(1000);
CREATE TABLE t_gating_one (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gating_one VALUES (1);
SELECT v FROM t_gating_one ANY LEFT JOIN t_gating_any USING (k)
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    join_algorithm = 'hash', join_any_take_last_row = 1, max_threads = 1;
DROP TABLE t_gating_any;
DROP TABLE t_gating_one;

-- An ANY join keeps one arbitrary matching row per key when the build side has duplicate keys,
-- so recomputing it independently on every node can give each node different rows. A subplan
-- that contains an ANY join must be computed once and broadcast, never replicated per node.
-- The stat hints make the fact side decisively large, so the dimension side of the outer join
-- gets a replicated requirement.
SELECT '-- 15. A subplan with an ANY join is broadcast, not recomputed per node';
-- Materialized statistics would displace the stat hints with the real (small) table sizes
-- and the plan would collapse to a single node, so keep the inserts from materializing them.
SET materialize_statistics_on_insert = 0;
DROP TABLE IF EXISTS t_gating_fact;
DROP TABLE IF EXISTS t_gating_dim1;
DROP TABLE IF EXISTS t_gating_dim2;
CREATE TABLE t_gating_fact (k UInt32, x UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gating_fact SELECT number % 1000, number FROM numbers(10000);
CREATE TABLE t_gating_dim1 (k UInt32, d UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_gating_dim1 SELECT number, number FROM numbers(1000);
CREATE TABLE t_gating_dim2 (d UInt32, name String) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_gating_dim2 SELECT number % 100, toString(number) FROM numbers(200);
SET param__internal_join_table_stat_hints = '{
    "t_gating_fact": { "cardinality": 100000000, "distinct_keys": { "k": 1000 } },
    "t_gating_dim1": { "cardinality": 1000, "distinct_keys": { "k": 1000, "d": 1000 } },
    "t_gating_dim2": { "cardinality": 200, "distinct_keys": { "d": 100 } }
}';
-- The outer probe runs without Cascades (`viewExplain` reads are rejected, see case 7); the
-- explained query enables it. Counts: replicated-subplan steps (must be 0), broadcasts (1).
SELECT countIf(explain LIKE '%(Replicated %'), countIf(explain LIKE '%BroadcastExchange%')
FROM (
    EXPLAIN
    SELECT dims.name, sum(f.x)
    FROM t_gating_fact AS f
    INNER JOIN (SELECT k, name FROM t_gating_dim1 ANY LEFT JOIN t_gating_dim2 USING (d)) AS dims ON f.k = dims.k
    GROUP BY dims.name
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
        distributed_plan_execute_locally = 1, join_algorithm = 'hash', max_rows_to_group_by = 0
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
-- The distributed join strategies shape and cost the plan for a hash join. When
-- `join_algorithm` allows no hash-family algorithm, the executed join would not match the
-- costed one, so the join stays single-node (the local variant runs whatever is allowed).
-- Counts: broadcast or shuffle hash joins (must be 0), joins in the plan (1, the local one).
SELECT '-- 16. A join with no hash-family algorithm allowed stays single-node';
SELECT countIf(explain LIKE '%Broadcast HashJoin%' OR explain LIKE '%Shuffle HashJoin%'), countIf(explain LIKE '%HashJoin%')
FROM (
    EXPLAIN
    SELECT sum(f.x)
    FROM t_gating_fact AS f
    INNER JOIN t_gating_dim1 AS d ON f.k = d.k
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
        distributed_plan_execute_locally = 1, join_algorithm = 'full_sorting_merge', max_rows_to_group_by = 0
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- A non-deterministic function gives different values when it is recomputed per node, so a
-- subplan that contains one must be computed on a single node and broadcast, never replicated.
SELECT '-- 17. A subplan with a non-deterministic function is broadcast, not recomputed per node';
SELECT countIf(explain LIKE '%(Replicated %'), countIf(explain LIKE '%BroadcastExchange%')
FROM (
    EXPLAIN
    SELECT sum(f.x + dims.r)
    FROM t_gating_fact AS f
    INNER JOIN (SELECT k, rand() AS r FROM t_gating_dim1) AS dims ON f.k = dims.k
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
        distributed_plan_execute_locally = 1, join_algorithm = 'hash', max_rows_to_group_by = 0
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SET param__internal_join_table_stat_hints = '';
DROP TABLE t_gating_fact;
DROP TABLE t_gating_dim1;
DROP TABLE t_gating_dim2;

DROP TABLE t_gating;
