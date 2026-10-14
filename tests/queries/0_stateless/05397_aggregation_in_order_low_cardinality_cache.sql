-- Aggregation in order over several LowCardinality keys caches aggregate states by the dictionary positions
-- of the keys that vary within a run of the sorting key. Distinct key tuples of a run must never share a
-- cache slot: the group count and min() / max() would change if they did. Each part has its own run length
-- of `a`. The SETTINGS of each side belong to its outermost SELECT: in a subquery they would not apply. Parallel
-- replicas are off on the in-order side, where the initiator's plan would not show the replicas' aggregation.
-- The projection table reaches the merge path of aggregation in order.

DROP TABLE IF EXISTS t_lc_in_order_cache;
CREATE TABLE t_lc_in_order_cache
(
    a LowCardinality(String),
    b LowCardinality(String),
    c LowCardinality(String),
    v UInt64
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_lc_in_order_cache SELECT concat('a', toString(number)), concat('b', toString(intHash32(number) % 14)), concat('c', toString(number % 3)), number FROM numbers(2000);
INSERT INTO t_lc_in_order_cache SELECT concat('a', toString(intDiv(number, 16) + 3000)), concat('b', toString((intHash32(number) + 5) % 14)), concat('c', toString((number + 1) % 3)), number FROM numbers(8000);
INSERT INTO t_lc_in_order_cache SELECT concat('a', toString(intDiv(number, 4096) + 9000)), concat('b', toString((intHash32(number) + 9) % 14)), concat('c', toString((number + 2) % 3)), number FROM numbers(16384);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT (count(), sum(cityHash64(a, b, cnt, s, mn, mx))) FROM (SELECT a, b, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b)
    SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
WHERE explain LIKE '%AggregatingInOrderTransform%';

SELECT
    (SELECT (count(), sum(cityHash64(a, b, cnt, s, mn, mx))) FROM (SELECT a, b, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b)
        SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
  = (SELECT (count(), sum(cityHash64(a, b, cnt, s, mn, mx))) FROM (SELECT a, b, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b)
        SETTINGS optimize_aggregation_in_order = 0);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT (count(), sum(cityHash64(b, a, cnt, s, mn, mx))) FROM (SELECT b, a, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY b, a)
    SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
WHERE explain LIKE '%AggregatingInOrderTransform%';

SELECT
    (SELECT (count(), sum(cityHash64(b, a, cnt, s, mn, mx))) FROM (SELECT b, a, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY b, a)
        SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
  = (SELECT (count(), sum(cityHash64(b, a, cnt, s, mn, mx))) FROM (SELECT b, a, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY b, a)
        SETTINGS optimize_aggregation_in_order = 0);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT (count(), sum(cityHash64(a, b, c, cnt, s, mn, mx))) FROM (SELECT a, b, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b, c)
    SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
WHERE explain LIKE '%AggregatingInOrderTransform%';

SELECT
    (SELECT (count(), sum(cityHash64(a, b, c, cnt, s, mn, mx))) FROM (SELECT a, b, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b, c)
        SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, enable_parallel_replicas = 0)
  = (SELECT (count(), sum(cityHash64(a, b, c, cnt, s, mn, mx))) FROM (SELECT a, b, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache GROUP BY a, b, c)
        SETTINGS optimize_aggregation_in_order = 0);

DROP TABLE IF EXISTS t_lc_in_order_cache_proj;
CREATE TABLE t_lc_in_order_cache_proj
(
    a LowCardinality(String),
    b LowCardinality(String),
    c LowCardinality(String),
    v UInt64,
    PROJECTION p (SELECT a, b, c, count(), sum(v), min(v), max(v) GROUP BY a, b, c)
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_lc_in_order_cache_proj SELECT concat('a', toString(number)), concat('b', toString(intHash32(number) % 14)), concat('c', toString(number % 3)), number FROM numbers(2000);
INSERT INTO t_lc_in_order_cache_proj SELECT concat('a', toString(intDiv(number, 16) + 3000)), concat('b', toString((intHash32(number) + 5) % 14)), concat('c', toString((number + 1) % 3)), number FROM numbers(8000);
INSERT INTO t_lc_in_order_cache_proj SELECT concat('a', toString(intDiv(number, 4096) + 9000)), concat('b', toString((intHash32(number) + 9) % 14)), concat('c', toString((number + 2) % 3)), number FROM numbers(16384);

SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT (count(), sum(cityHash64(a, c, cnt, s, mn, mx))) FROM (SELECT a, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache_proj GROUP BY a, c)
    SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0)
WHERE explain LIKE '%AggregatingInOrderTransform%';

SELECT
    (SELECT (count(), sum(cityHash64(a, c, cnt, s, mn, mx))) FROM (SELECT a, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache_proj GROUP BY a, c)
        SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, optimize_use_projections = 1, force_optimize_projection = 1, enable_parallel_replicas = 0)
  = (SELECT (count(), sum(cityHash64(a, c, cnt, s, mn, mx))) FROM (SELECT a, c, count() AS cnt, sum(v) AS s, min(v) AS mn, max(v) AS mx FROM t_lc_in_order_cache_proj GROUP BY a, c)
        SETTINGS optimize_aggregation_in_order = 0, optimize_use_projections = 0);

DROP TABLE t_lc_in_order_cache;
DROP TABLE t_lc_in_order_cache_proj;
