-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks the shape of the local query plan.

DROP TABLE IF EXISTS t_lazy_final_limit;
DROP TABLE IF EXISTS t_lazy_final_limit_ver;

CREATE TABLE t_lazy_final_limit (k UInt64, v UInt64, payload String) ENGINE = ReplacingMergeTree ORDER BY k;

SYSTEM STOP MERGES t_lazy_final_limit;

INSERT INTO t_lazy_final_limit SELECT number, if(number < 1000, 7, 999), 'v1_' || toString(number) FROM numbers(100000);
INSERT INTO t_lazy_final_limit SELECT number, if(number < 1000, 7, 999), 'v2_' || toString(number) FROM numbers(100000);

-- The test relies on settings that are randomized by the test runner: pin them.
SET enable_analyzer = 1, max_threads = 4;
SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SET optimize_move_to_prewhere = 1;

-- FINAL with a filter and a small LIMIT without ORDER BY: lazy materialization applies
-- (the filter cannot be moved to PREWHERE, so this is the only way to avoid reading
-- all columns for every scanned row).
SELECT 'FINAL filter limit:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL WHERE v = 7 LIMIT 10);

-- Without FINAL the filter is served by PREWHERE: not applied.
SELECT 'no FINAL:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit WHERE v = 7 LIMIT 10);

-- Without a filter: not applied.
SELECT 'FINAL no filter:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL LIMIT 10);

-- Without a limit: not applied.
SELECT 'FINAL no limit:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL WHERE v = 7);

-- A limit above query_plan_max_limit_for_lazy_materialization: not applied.
SELECT 'FINAL big limit:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL WHERE v = 7 LIMIT 100000);

-- An explicit PREWHERE is carried by the reading step itself, with no FilterStep above it:
-- lazy materialization applies.
SELECT 'FINAL prewhere limit:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL PREWHERE v = 7 LIMIT 10);

-- Every returned row must be the FINAL winner (the second insert), keys distinct.
SELECT 'winners:', count(), countIf(payload LIKE 'v2\_%'), uniqExact(k)
FROM (SELECT k, payload FROM t_lazy_final_limit FINAL WHERE v = 7 LIMIT 10);

SELECT 'prewhere winners:', count(), countIf(payload LIKE 'v2\_%'), uniqExact(k)
FROM (SELECT k, payload FROM t_lazy_final_limit FINAL PREWHERE v = 7 LIMIT 10);

-- The full matching set must be the same with and without the optimization.
SELECT 'full set equal:',
    (SELECT (count(), sum(cityHash64(payload)), sum(k)) FROM (SELECT k, payload FROM t_lazy_final_limit FINAL WHERE v = 7 LIMIT 10000))
    =
    (SELECT (count(), sum(cityHash64(payload)), sum(k)) FROM (SELECT k, payload FROM t_lazy_final_limit FINAL WHERE v = 7 LIMIT 10000 SETTINGS query_plan_optimize_lazy_materialization = 0));

SELECT 'prewhere full set equal:',
    (SELECT (count(), sum(cityHash64(payload)), sum(k)) FROM (SELECT k, payload FROM t_lazy_final_limit FINAL PREWHERE v = 7 LIMIT 10000))
    =
    (SELECT (count(), sum(cityHash64(payload)), sum(k)) FROM (SELECT k, payload FROM t_lazy_final_limit FINAL PREWHERE v = 7 LIMIT 10000 SETTINGS query_plan_optimize_lazy_materialization = 0));

-- Version and is_deleted columns are handled by the FINAL merge on the main branch.
CREATE TABLE t_lazy_final_limit_ver (k UInt64, ver UInt64, is_del UInt8, v UInt64, payload String)
ENGINE = ReplacingMergeTree(ver, is_del) ORDER BY k;

SYSTEM STOP MERGES t_lazy_final_limit_ver;

INSERT INTO t_lazy_final_limit_ver SELECT number, 1, 0, if(number < 1000, 7, 999), 'v1_' || toString(number) FROM numbers(100000);
INSERT INTO t_lazy_final_limit_ver SELECT number, 2, number % 4 = 0, if(number < 1000, 7, 999), 'v2_' || toString(number) FROM numbers(500);

SELECT 'is_deleted plan:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit_ver FINAL WHERE v = 7 LIMIT 2000);

-- 1000 matching keys minus 125 deleted by the second insert.
SELECT 'is_deleted rows:', count(), countIf(k < 500 AND payload NOT LIKE 'v2\_%')
FROM (SELECT k, payload FROM t_lazy_final_limit_ver FINAL WHERE v = 7 LIMIT 2000);

-- A row policy is a reader-embedded filter as well: lazy materialization applies even
-- without WHERE, and the returned rows respect the policy.
CREATE ROW POLICY policy_04540 ON t_lazy_final_limit FOR SELECT USING k < 50000 TO ALL;

SELECT 'row policy plan:', countIf(explain LIKE '%LazilyReadFromMergeTree%') > 0
FROM (EXPLAIN SELECT payload FROM t_lazy_final_limit FINAL LIMIT 10);

SELECT 'row policy rows:', count(), countIf(payload LIKE 'v2\_%'), max(k) < 50000
FROM (SELECT k, payload FROM t_lazy_final_limit FINAL LIMIT 10);

DROP ROW POLICY policy_04540 ON t_lazy_final_limit;

DROP TABLE t_lazy_final_limit;
DROP TABLE t_lazy_final_limit_ver;
