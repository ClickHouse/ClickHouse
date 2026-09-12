-- Tags: distributed

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99370
-- With `distributed_product_mode = 'global'` (or `prefer_global_in_and_join`), every distributed table
-- found under an `IN` function asked to rewrite that function to its `GLOBAL` counterpart. A `UNION` of
-- distributed tables in the right argument of `IN` therefore rewrote the same function twice and failed
-- with `Invalid local IN function name globalIn` (`BAD_ARGUMENTS`).

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;

CREATE TABLE t_local (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local VALUES (1), (2);

-- `rand()` as the sharding key, so that a randomized `optimize_skip_unused_shards` cannot prune a shard.
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local, rand());

SET enable_analyzer = 1;
SET distributed_product_mode = 'global';

SELECT 'IN with UNION ALL';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist) ORDER BY x;

SELECT 'IN with UNION DISTINCT in a CTE';
WITH u AS (SELECT x FROM t_dist UNION DISTINCT SELECT x FROM t_dist)
SELECT x FROM t_dist WHERE x IN (SELECT x FROM u) ORDER BY x;

SELECT 'NOT IN with UNION ALL';
SELECT x FROM t_dist WHERE x NOT IN (SELECT x FROM t_dist WHERE x = 1 UNION ALL SELECT x FROM t_dist WHERE x = 1) ORDER BY x;

SELECT 'IN with three distributed tables in the subquery';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist) ORDER BY x;

SELECT 'Nested IN, both with UNION ALL';
SELECT x FROM t_dist WHERE x IN (
    SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist)
    UNION ALL
    SELECT x FROM t_dist
) ORDER BY x;

SELECT 'JOIN with UNION ALL of distributed tables in the left table expression';
SELECT count() FROM t_dist WHERE x IN (
    SELECT a.x FROM (SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist) AS a JOIN t_local AS b ON a.x = b.x
);

SELECT 'IN with UNION ALL and prefer_global_in_and_join';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_dist UNION ALL SELECT x FROM t_dist) ORDER BY x
SETTINGS distributed_product_mode = 'deny', prefer_global_in_and_join = 1;

DROP TABLE t_dist;
DROP TABLE t_local;
