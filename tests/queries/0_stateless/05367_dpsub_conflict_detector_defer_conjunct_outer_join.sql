-- A conjunct of an inner join may be placed below an outer join nested under it only onto that
-- join's preserved side. In `(t_a JOIN t_b) RIGHT JOIN t_c ... INNER JOIN t_d ON ... AND t_a.flag = 0`
-- the filter on `t_a` must wait until the `RIGHT JOIN` has padded the unmatched row of `t_c`:
-- applied inside `t_a JOIN t_b`, it would remove the match and turn it into a padded row.
-- A two-table conjunct (`t_a.flag = t_b.flag`) has to wait the same way, without preventing
-- `t_a JOIN t_b` itself from being planned.
-- Every query must return the same rows as without reordering.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET use_hash_table_stats_for_join_reordering = 0;
-- Without the other plan optimizations, the `RIGHT JOIN` is not turned into an inner join.
SET query_plan_enable_optimizations = 0;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;
DROP TABLE IF EXISTS t_na;
DROP TABLE IF EXISTS t_nb;
DROP TABLE IF EXISTS t_nc;
DROP TABLE IF EXISTS t_nd;

-- `t_c` and `t_d` have the key 1, which matches `t_a`, and the key 2, which does not.
CREATE TABLE t_a (k UInt32, flag UInt32) ENGINE = Memory;
CREATE TABLE t_b (k UInt32, flag UInt32) ENGINE = Memory;
CREATE TABLE t_c (k UInt32) ENGINE = Memory;
CREATE TABLE t_d (k UInt32) ENGINE = Memory;
INSERT INTO t_a VALUES (1, 1);
INSERT INTO t_b VALUES (1, 2);
INSERT INTO t_c VALUES (1), (2);
INSERT INTO t_d VALUES (1), (2);

CREATE TABLE t_na (k Nullable(UInt32), flag Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nb (k Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nc (k Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nd (k Nullable(UInt32)) ENGINE = Memory;
INSERT INTO t_na VALUES (1, 1);
INSERT INTO t_nb VALUES (1);
INSERT INTO t_nc VALUES (1), (2);
INSERT INTO t_nd VALUES (1), (2);

SELECT '-- single-table conjunct above RIGHT JOIN';
SELECT t_a.k, t_a.flag, t_b.k, t_c.k, t_d.k
FROM t_a INNER JOIN t_b ON t_a.k = t_b.k
RIGHT JOIN t_c ON t_a.k = t_c.k
INNER JOIN t_d ON t_c.k = t_d.k AND t_a.flag = 0
ORDER BY ALL;

SELECT '-- single-table conjunct above FULL JOIN';
SELECT t_a.k, t_a.flag, t_b.k, t_c.k, t_d.k
FROM t_a INNER JOIN t_b ON t_a.k = t_b.k
FULL JOIN t_c ON t_a.k = t_c.k
INNER JOIN t_d ON t_c.k = t_d.k AND t_a.flag = 0
ORDER BY ALL;

SELECT '-- single-table conjunct above RIGHT JOIN, semi join below';
SELECT t_a.k, t_a.flag, t_c.k, t_d.k
FROM t_a LEFT SEMI JOIN t_b ON t_a.k = t_b.k
RIGHT JOIN t_c ON t_a.k = t_c.k
INNER JOIN t_d ON t_c.k = t_d.k AND t_a.flag = 0
ORDER BY ALL;

SELECT '-- two-table conjunct above RIGHT JOIN';
SELECT t_a.k, t_a.flag, t_b.k, t_c.k, t_d.k
FROM t_a INNER JOIN t_b ON t_a.k = t_b.k
RIGHT JOIN t_c ON t_a.k = t_c.k
INNER JOIN t_d ON t_c.k = t_d.k AND t_a.flag = t_b.flag
ORDER BY ALL;

SELECT '-- single-table conjunct above RIGHT JOIN, join_use_nulls = 1';
SELECT t_na.k, t_na.flag, t_nb.k, t_nc.k, t_nd.k
FROM t_na INNER JOIN t_nb ON t_na.k = t_nb.k
RIGHT JOIN t_nc ON t_na.k = t_nc.k
INNER JOIN t_nd ON t_nc.k = t_nd.k AND ifNull(t_na.flag, 0) = 0
ORDER BY ALL
SETTINGS join_use_nulls = 1;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
DROP TABLE t_na;
DROP TABLE t_nb;
DROP TABLE t_nc;
DROP TABLE t_nd;
