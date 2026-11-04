-- An inner join's ON clause is split into conjuncts that the join order optimizer places on their
-- own. In `t_a LEFT JOIN t_b ... INNER JOIN t_c ON t_b.k = t_c.bk AND t_a.k = t_c.ak`, the conjunct
-- `t_b.k = t_c.bk` alone could join `t_b` with `t_c` first, and then the outer join would be applied
-- to that result: `t_a LEFT JOIN (t_b JOIN t_c)`, which loses the row of `t_a` without a match in
-- `t_b`. The conflict detector forbids it, and the conjunct placed on its own has to obey that too.
-- Every query must return the same rows as without reordering.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET query_plan_convert_outer_join_to_inner_join = 0;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_na;
DROP TABLE IF EXISTS t_nb;
DROP TABLE IF EXISTS t_nc;

CREATE TABLE t_a (k UInt32) ENGINE = Memory;
CREATE TABLE t_b (k UInt32) ENGINE = Memory;
CREATE TABLE t_c (bk UInt32, ak UInt32) ENGINE = Memory;
INSERT INTO t_a VALUES (1);
INSERT INTO t_b VALUES (2);
INSERT INTO t_c VALUES (0, 1);

CREATE TABLE t_na (k Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nb (k Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nc (bk Nullable(UInt32), ak Nullable(UInt32)) ENGINE = Memory;
INSERT INTO t_na VALUES (1);
INSERT INTO t_nb VALUES (2);
INSERT INTO t_nc VALUES (0, 1);

-- With `join_use_nulls = 0` the unmatched row of `t_a` gets `t_b.k = 0`, which matches `t_c.bk = 0`.
SELECT '-- LEFT JOIN';
SELECT t_a.k, t_b.k, t_c.bk, t_c.ak
FROM t_a LEFT JOIN t_b ON t_a.k = t_b.k
INNER JOIN t_c ON t_b.k = t_c.bk AND t_a.k = t_c.ak
SETTINGS join_use_nulls = 0;

SELECT '-- FULL JOIN';
SELECT t_a.k, t_b.k, t_c.bk, t_c.ak
FROM t_a FULL JOIN t_b ON t_a.k = t_b.k
INNER JOIN t_c ON t_b.k = t_c.bk AND t_a.k = t_c.ak
ORDER BY ALL
SETTINGS join_use_nulls = 0;

-- With `join_use_nulls = 1` the unmatched row gets `NULL`, which `ifNull` turns into a match.
SELECT '-- LEFT JOIN, join_use_nulls = 1';
SELECT t_na.k, t_nb.k, t_nc.bk, t_nc.ak
FROM t_na LEFT JOIN t_nb ON t_na.k = t_nb.k
INNER JOIN t_nc ON ifNull(t_nb.k, 0) = t_nc.bk AND t_na.k = t_nc.ak
SETTINGS join_use_nulls = 1;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_na;
DROP TABLE t_nb;
DROP TABLE t_nc;
