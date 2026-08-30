-- `enable_group_by_top_k_optimization` became default-on in 26.8; the
-- `compatibility` setting must restore the old default without blocking an
-- explicit override.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS t_top_k_compat;
CREATE TABLE t_top_k_compat (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_top_k_compat SELECT number, number FROM numbers(1000);

SELECT 'default: optimized';
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_compat GROUP BY k ORDER BY k ASC LIMIT 10);

SELECT 'compatibility 26.7: not optimized';
SET compatibility = '26.7';
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_compat GROUP BY k ORDER BY k ASC LIMIT 10);

SELECT 'explicit enable overrides compatibility';
SET enable_group_by_top_k_optimization = 1;
SELECT countIf(explain LIKE '%Top-K%') FROM (EXPLAIN actions = 1
    SELECT k, sum(v) FROM t_top_k_compat GROUP BY k ORDER BY k ASC LIMIT 10);

DROP TABLE t_top_k_compat;
