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
