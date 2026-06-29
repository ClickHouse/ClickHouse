-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_106573_minmax;
CREATE TABLE t_106573_minmax (c0 UInt8) ENGINE = MergeTree ORDER BY tuple() PARTITION BY c0;
INSERT INTO t_106573_minmax VALUES (1),(2);

SELECT '--- minmax_count_projection: WHERE c0 GROUP BY c0 (2 partitions) ---';
SELECT c0, count() FROM t_106573_minmax WHERE c0 GROUP BY c0 ORDER BY c0;

TRUNCATE TABLE t_106573_minmax;
INSERT INTO t_106573_minmax VALUES (1),(2),(3),(4),(5);

SELECT '--- minmax_count_projection: WHERE c0 GROUP BY c0 (5 partitions) ---';
SELECT c0, count() FROM t_106573_minmax WHERE c0 GROUP BY c0 ORDER BY c0;

SELECT '--- baseline (optimize_use_implicit_projections = 0) ---';
SELECT c0, count() FROM t_106573_minmax WHERE c0 GROUP BY c0 ORDER BY c0
SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_106573_minmax;

DROP TABLE IF EXISTS t_106573_agg_proj;
CREATE TABLE t_106573_agg_proj
(
    c0 UInt8,
    PROJECTION p (SELECT c0, count() GROUP BY c0)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_106573_agg_proj VALUES (1),(2);

SELECT '--- aggregate projection: WHERE c0 GROUP BY c0 ---';
SELECT c0, count() FROM t_106573_agg_proj WHERE c0 GROUP BY c0 ORDER BY c0;

DROP TABLE t_106573_agg_proj;

DROP TABLE IF EXISTS t_106573_norm_proj;
CREATE TABLE t_106573_norm_proj
(
    c0 UInt8,
    x UInt32,
    PROJECTION np (SELECT c0, x ORDER BY c0)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_106573_norm_proj VALUES (1, 100),(2, 200);

SELECT '--- normal projection: SELECT c0, x WHERE c0 ---';
SELECT c0, x FROM t_106573_norm_proj WHERE c0 ORDER BY c0;

DROP TABLE t_106573_norm_proj;

-- A surviving (non-removed) filter column must be the value that passed the filter, not a
-- fresh re-evaluation. When the filter expression is non-deterministic, the normal-projection
-- rewrite turns it into projection PREWHERE and reuses the computed value for the post-read
-- expression, so a row that passed `WHERE r` is always output with `r = 1`, never a recomputed
-- `r = 0`. `min(r) = 1` over all surviving rows asserts that contract. `force_optimize_projection`
-- guarantees the projection (and thus the rewrite) is actually used.

DROP TABLE IF EXISTS t_106573_nd;
CREATE TABLE t_106573_nd
(
    c0 UInt8,
    x UInt32,
    PROJECTION np (SELECT c0, x ORDER BY c0)
) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_106573_nd SELECT number % 256, number FROM numbers(10000);

SELECT '--- normal projection: non-deterministic surviving filter keeps the value that passed the filter ---';
WITH toUInt8(rand() % 2) AS r
SELECT min(r) FROM t_106573_nd WHERE r
SETTINGS force_optimize_projection = 1, enable_parallel_replicas = 0;

DROP TABLE t_106573_nd;
