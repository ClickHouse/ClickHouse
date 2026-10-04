-- EXPLAIN ANALYZE output is non-deterministic, so only the presence of the
-- memory line under the Aggregating step and a non-zero value are checked.

DROP TABLE IF EXISTS t_agg_proj_merge_only;
SET enable_analyzer = 1;

SELECT
    countIf(explain LIKE '%Aggregating%') >= 1,
    countIf(explain LIKE '%Memory: bytes %') = 1,
    countIf(explain LIKE '%Memory: bytes %' AND explain NOT LIKE '%bytes 0.00 B%' AND explain NOT LIKE '%bytes 0 B%') = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k);

-- An aggregation served entirely from an aggregate projection prints no memory line; the outer aggregation still does.
SET enable_parallel_replicas = 0;
DROP TABLE IF EXISTS t_agg_proj_merge_only;
CREATE TABLE t_agg_proj_merge_only (k UInt64, v UInt64, PROJECTION p (SELECT k, uniqExact(v) GROUP BY k)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_agg_proj_merge_only SELECT number % 1000, number FROM numbers(100000);
SELECT
    countIf(explain LIKE '%AggregatingProjection%') = 0,
    countIf(explain LIKE '%Aggregating%') = 2,
    countIf(explain LIKE '%Memory: bytes %') = 1
FROM (EXPLAIN ANALYZE SELECT max(u) FROM (SELECT k, uniqExact(v) AS u FROM t_agg_proj_merge_only GROUP BY k)
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);
DROP TABLE t_agg_proj_merge_only;
