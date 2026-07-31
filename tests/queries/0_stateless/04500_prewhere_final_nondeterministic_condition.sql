-- Conditions with non-deterministic functions like rand must stay in WHERE under FINAL,
-- otherwise row versions of the same key get filtered differently and a stale row can win the merge
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;

DROP TABLE IF EXISTS t_prewhere_final_rand;

CREATE TABLE t_prewhere_final_rand (k Int32, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;

INSERT INTO t_prewhere_final_rand SELECT number, 'old', 1 FROM numbers(10000);
INSERT INTO t_prewhere_final_rand SELECT number, 'new', 2 FROM numbers(10000);

SELECT '= deterministic sorting-key condition is moved =';
SELECT replaceRegexpAll(explain, '__table1\.|_UInt8|_UInt16', '') FROM (EXPLAIN actions=1 SELECT * FROM t_prewhere_final_rand FINAL WHERE k > 100) WHERE explain LIKE '%Prewhere%';

SELECT '= non-deterministic condition is not moved =';
SELECT count() FROM (EXPLAIN actions=1 SELECT * FROM t_prewhere_final_rand FINAL WHERE (k + rand()) % 2 = 0) WHERE explain LIKE '%Prewhere filter%';

-- stale rows must never survive FINAL because of pre-merge filtering
SELECT countIf(data = 'old') FROM t_prewhere_final_rand FINAL WHERE (k + rand()) % 2 = 0;

-- FunctionCapture must report the determinism of the lambda body, not the default flags
SELECT '= non-deterministic condition inside a lambda is not moved =';
SELECT count() FROM (EXPLAIN actions=1 SELECT * FROM t_prewhere_final_rand FINAL WHERE arrayExists(x -> (x + rand()) % 2 = 0, [k])) WHERE explain LIKE '%Prewhere filter%';

SELECT countIf(data = 'old') FROM t_prewhere_final_rand FINAL WHERE arrayExists(x -> (x + rand()) % 2 = 0, [k]);

DROP TABLE t_prewhere_final_rand;

-- query-scoped constants like now are folded before the optimizer runs, so such filters keep moving
SELECT '= filter with now is still moved =';

DROP TABLE IF EXISTS t_prewhere_final_now;

CREATE TABLE t_prewhere_final_now (ts DateTime, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY ts;

INSERT INTO t_prewhere_final_now SELECT toDateTime('2026-01-01 00:00:00') - INTERVAL number HOUR, 'x', 1 FROM numbers(100);

SELECT count() > 0 FROM (EXPLAIN actions=1 SELECT * FROM t_prewhere_final_now FINAL WHERE ts >= now() - INTERVAL 1 DAY) WHERE explain LIKE '%Prewhere filter%';

DROP TABLE t_prewhere_final_now;

-- runtime filters are stable within a query, so they are still eligible for PREWHERE under FINAL
SELECT '= join runtime filter on the sorting key is still moved =';

DROP TABLE IF EXISTS t_prewhere_final_rf;
DROP TABLE IF EXISTS t_prewhere_final_rf_right;

CREATE TABLE t_prewhere_final_rf (k Int32, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
CREATE TABLE t_prewhere_final_rf_right (k Int32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_prewhere_final_rf SELECT number, 'x', 1 FROM numbers(100000);
INSERT INTO t_prewhere_final_rf_right SELECT number * 100 FROM numbers(100);

SET enable_join_runtime_filters = 1;
SET query_plan_join_swap_table = false;

SELECT count() FROM (EXPLAIN actions=1 SELECT t_prewhere_final_rf.data FROM t_prewhere_final_rf FINAL INNER JOIN t_prewhere_final_rf_right ON t_prewhere_final_rf.k = t_prewhere_final_rf_right.k) WHERE explain LIKE '%Prewhere filter column: __applyFilter%';

DROP TABLE t_prewhere_final_rf;
DROP TABLE t_prewhere_final_rf_right;
