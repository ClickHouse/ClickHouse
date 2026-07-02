-- Non-deterministic conditions must not be moved to PREWHERE under FINAL: they could filter
-- row versions of one dedup group differently and change which row survives the merge.
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

DROP TABLE t_prewhere_final_rand;
