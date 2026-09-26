-- Under parallel replicas the materialized side of a join is prepared on the initiator as a global join,
-- and a comma join is a join like any other there: it used to be refused with `Unexpected global join
-- kind: COMMA`. The results must match the plain execution.

DROP TABLE IF EXISTS t_pr_comma_left;
DROP TABLE IF EXISTS t_pr_comma_right;
DROP VIEW IF EXISTS v_pr_comma_right;
CREATE TABLE t_pr_comma_left (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pr_comma_right (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_comma_left SELECT number, number * 10 FROM numbers(100);
INSERT INTO t_pr_comma_right SELECT number * 2, number FROM numbers(50);
CREATE VIEW v_pr_comma_right AS SELECT k, w FROM t_pr_comma_right WHERE w % 2 = 0;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas', parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT '-- comma join with a view, keys in WHERE';
SELECT count(), sum(v), sum(w) FROM t_pr_comma_left, v_pr_comma_right WHERE t_pr_comma_left.k = v_pr_comma_right.k;
SELECT count(), sum(v), sum(w) FROM t_pr_comma_left, v_pr_comma_right WHERE t_pr_comma_left.k = v_pr_comma_right.k
    SETTINGS enable_parallel_replicas = 0;

SELECT '-- comma join with a subquery that could stay local';
SELECT count(), sum(v), sum(w) FROM t_pr_comma_left, (SELECT k, w FROM t_pr_comma_right WHERE w % 2 = 0) AS r WHERE t_pr_comma_left.k = r.k
    SETTINGS parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(v), sum(w) FROM t_pr_comma_left, (SELECT k, w FROM t_pr_comma_right WHERE w % 2 = 0) AS r WHERE t_pr_comma_left.k = r.k
    SETTINGS enable_parallel_replicas = 0;

SELECT '-- CROSS JOIN with a subquery';
SELECT count(), sum(v) FROM t_pr_comma_left CROSS JOIN (SELECT k FROM t_pr_comma_right WHERE k < 10) AS r
    SETTINGS parallel_replicas_prefer_local_join = 0;
SELECT count(), sum(v) FROM t_pr_comma_left CROSS JOIN (SELECT k FROM t_pr_comma_right WHERE k < 10) AS r
    SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_comma_right;
DROP TABLE t_pr_comma_left;
DROP TABLE t_pr_comma_right;
