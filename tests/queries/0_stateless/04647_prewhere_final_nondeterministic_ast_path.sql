-- Same guarantee as 04500, but for the legacy AST-level `optimize_move_to_prewhere`:
-- a built-in non-deterministic function like `rand` must not be moved to PREWHERE under FINAL,
-- otherwise row versions of the same key get filtered differently and a stale row can win the merge.
SET enable_analyzer = 0;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 0;
SET optimize_move_to_prewhere_if_final = 1;

DROP TABLE IF EXISTS t_prewhere_final_rand_ast;

CREATE TABLE t_prewhere_final_rand_ast (k Int32, data String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;

INSERT INTO t_prewhere_final_rand_ast SELECT number, 'old', 1 FROM numbers(10000);
INSERT INTO t_prewhere_final_rand_ast SELECT number, 'new', 2 FROM numbers(10000);

SELECT '= deterministic sorting-key condition is moved =';
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM t_prewhere_final_rand_ast FINAL WHERE k + 1 > 100) WHERE explain LIKE '%PREWHERE%';

SELECT '= non-deterministic condition is not moved =';
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM t_prewhere_final_rand_ast FINAL WHERE (k + rand()) % 2 = 0) WHERE explain LIKE '%PREWHERE%';

-- stale rows must never survive FINAL because of pre-merge filtering
SELECT countIf(data = 'old') FROM t_prewhere_final_rand_ast FINAL WHERE (k + rand()) % 2 = 0;

DROP TABLE t_prewhere_final_rand_ast;
