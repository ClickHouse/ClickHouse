-- Tags: no-old-analyzer
-- Baseline contract for `IN (subquery)` under `make_distributed_plan`:
-- `adjustSettingsForMakeDistributedPlan` forces `rewrite_in_to_join`, so plain `IN` executes
-- as a distributed join. With the rewrite disabled, the set is built once on the initiator
-- during planning and its values ship with the worker tasks (`GLOBAL IN` semantics).

CREATE TABLE t_big (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_small (id UInt64, val UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_big SELECT number, number FROM numbers(1000);
INSERT INTO t_small SELECT number, number * 2 FROM numbers(100);

SET enable_analyzer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET use_index_for_in_with_subqueries = 1, use_query_condition_cache = 0;

SELECT '-- literal IN ships as TupleValues';
SELECT count() FROM t_big WHERE k IN (1, 2, 3, 1000000);

SELECT '-- IN (subquery) works via the forced join rewrite';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50);

SELECT '-- the direct set path: built once at planning, values shipped to tasks';
SELECT count() FROM t_big WHERE k IN (SELECT val FROM t_small WHERE id < 50)
    SETTINGS allow_experimental_correlated_subqueries = 0, rewrite_in_to_join = 0;

SELECT '-- GLOBAL IN over local tables behaves as plain IN';
SELECT count() FROM t_big WHERE k GLOBAL IN (SELECT val FROM t_small WHERE id < 50);

SELECT '-- GLOBAL JOIN executes as a distributed join (no external table under the analyzer)';
SELECT count() FROM t_big GLOBAL ANY LEFT JOIN t_small ON t_big.k = t_small.val;

DROP TABLE t_big;
DROP TABLE t_small;
