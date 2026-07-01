-- A partitioned (shuffle) outer join must not advertise its output as distributed by the
-- null-extended side's key.  Unmatched rows carry a null/default-extended key there, so a
-- downstream operator keyed on that side would skip a required re-distribution and split
-- equal keys across nodes.  Only the preserved side(s) may be advertised; FULL preserves
-- neither.  Results must match the non-distributed baseline.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
-- Pin it to 0 so the asserted distributed plans are exercised.
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":1000,"network_weight":1,"exchange_fixed_overhead":1,"work_weight":1}';

DROP TABLE IF EXISTS oj_l;
DROP TABLE IF EXISTS oj_r;
DROP TABLE IF EXISTS oj_s;
CREATE TABLE oj_l (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE oj_r (k UInt64, b UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE oj_s (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO oj_l SELECT number, number FROM numbers(100000);
INSERT INTO oj_r SELECT number * 2, number FROM numbers(100000);
INSERT INTO oj_s SELECT number, number FROM numbers(100000);

-- The downstream join is keyed on the RIGHT join's null-extended left side, so it must not
-- reuse the outer join output as already-partitioned by that key (it must broadcast or
-- re-shuffle).  Without the fix the inner join would be a Shuffle join over the un-redistributed
-- outer-join output.
SELECT '-- RIGHT JOIN then join on the null-extended (left) key';
EXPLAIN PLAN
SELECT count() FROM (SELECT oj_l.k AS jk FROM oj_l RIGHT JOIN oj_r ON oj_l.k = oj_r.k) j JOIN oj_s ON j.jk = oj_s.k;

SELECT '-- results match the non-distributed baseline';
SELECT count() FROM (SELECT oj_l.k AS jk FROM oj_l RIGHT JOIN oj_r ON oj_l.k = oj_r.k) j JOIN oj_s ON j.jk = oj_s.k;
SELECT count() FROM (SELECT oj_r.k AS jk FROM oj_l LEFT JOIN oj_r ON oj_l.k = oj_r.k) j JOIN oj_s ON j.jk = oj_s.k;
SELECT sum(cnt) FROM (SELECT oj_l.k AS lk, count() AS cnt FROM oj_l FULL JOIN oj_r ON oj_l.k = oj_r.k GROUP BY lk);
SELECT sum(cnt) FROM (SELECT oj_r.k AS rk, count() AS cnt FROM oj_l FULL JOIN oj_r ON oj_l.k = oj_r.k GROUP BY rk);

DROP TABLE oj_l;
DROP TABLE oj_r;
DROP TABLE oj_s;
