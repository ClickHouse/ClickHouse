-- Tags: no-old-analyzer
-- Baseline contract for `IN (subquery)` under `make_distributed_plan`:
-- `adjustSettingsForMakeDistributedPlan` forces `rewrite_in_to_join`, so plain `IN` executes
-- as a distributed join, while the direct set path (rewrite disabled below) is rejected at
-- the fragment cut. The rejection pins are temporary until deferred set delivery lands.

CREATE TABLE t_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_small (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_big SELECT number, number FROM numbers(1000);
INSERT INTO t_small SELECT number, number * 2 FROM numbers(100);

SET enable_analyzer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;

SELECT '-- literal IN ships as TupleValues';
SELECT count() FROM t_big WHERE k IN (1, 2, 3, 1000000);

SELECT '-- IN (subquery) works via the forced join rewrite';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50);

SELECT '-- the direct set path is rejected at the cut (temporary pin)';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50)
    SETTINGS allow_experimental_correlated_subqueries = 0, rewrite_in_to_join = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- GLOBAL IN takes the set path and is rejected the same way';
SELECT count() FROM t_big WHERE k GLOBAL IN (SELECT val FROM t_small WHERE id < 50); -- { serverError SUPPORT_IS_DISABLED }

SELECT '-- GLOBAL JOIN executes as a distributed join (no external table under the analyzer)';
SELECT count() FROM t_big GLOBAL ANY LEFT JOIN t_small ON t_big.k = t_small.val;

DROP TABLE t_big;
DROP TABLE t_small;
