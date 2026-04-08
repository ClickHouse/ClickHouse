-- Edge case tests for transitive predicate inference.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_join_transitive_predicates = 1;

DROP TABLE IF EXISTS e1;
DROP TABLE IF EXISTS e2;
DROP TABLE IF EXISTS e3;

CREATE TABLE e1 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE e2 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;
CREATE TABLE e3 (key UInt64, val UInt64) ENGINE = MergeTree ORDER BY key;

INSERT INTO e1 SELECT number, number * 100 FROM numbers(10);
INSERT INTO e2 SELECT number, number * 200 FROM numbers(10);
INSERT INTO e3 SELECT number, number * 300 FROM numbers(10);

-- ==========================================================================
-- Case 7: Non-INPUT expression in predicate (e1.key + 1 = e2.key)
-- Should NOT form equivalence classes (only simple column refs do).
-- Must not crash or produce wrong results.
-- ==========================================================================

SELECT '-- Case 7: non-INPUT expression';
SELECT e1.key, e2.key, e3.key
FROM e1
INNER JOIN e2 ON e1.key + 1 = e2.key
INNER JOIN e3 ON e2.key = e3.key
ORDER BY e1.key LIMIT 5;

SELECT e1.key, e2.key, e3.key
FROM e1
INNER JOIN e2 ON e1.key + 1 = e2.key
INNER JOIN e3 ON e2.key = e3.key
ORDER BY e1.key LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 9: Chain with WHERE filter after join
-- The filter should still apply correctly after transitive optimization.
-- ==========================================================================

SELECT '-- Case 9: WHERE filter after join';
SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key AND e1.key > 5
ORDER BY e1.key;

SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key AND e1.key > 5
ORDER BY e1.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 10: Self-join transitivity
-- Same table aliased three times.
-- ==========================================================================

SELECT '-- Case 10: self-join';
SELECT a.key, b.key, c.key
FROM e1 AS a, e1 AS b, e1 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key LIMIT 5;

SELECT a.key, b.key, c.key
FROM e1 AS a, e1 AS b, e1 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key LIMIT 5
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 11: Subquery as join input
-- One relation is a filtered subquery. Equivalences should still work.
-- ==========================================================================

SELECT '-- Case 11: subquery input';
SELECT a.key, b.key, c.key
FROM e1 AS a, (SELECT key, val FROM e2 WHERE val < 1500) AS b, e3 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key;

SELECT a.key, b.key, c.key
FROM e1 AS a, (SELECT key, val FROM e2 WHERE val < 1500) AS b, e3 AS c
WHERE a.key = b.key AND b.key = c.key
ORDER BY a.key
SETTINGS enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 12: Join reordering disabled
-- Dedup and synthesis must still work when the join order optimizer
-- keeps the original plan unchanged (query_plan_optimize_join_order_limit = 0).
-- ==========================================================================

SELECT '-- Case 12: reordering disabled';
SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key
ORDER BY e1.key LIMIT 5
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT e1.key, e2.key, e3.key
FROM e1, e2, e3
WHERE e1.key = e2.key AND e2.key = e3.key
ORDER BY e1.key LIMIT 5
SETTINGS query_plan_optimize_join_order_limit = 0, enable_join_transitive_predicates = 0;

-- ==========================================================================
-- Case 13: Mixed column types
-- When types differ, implicit casts produce non-INPUT nodes that
-- resolveInput skips. Transitivity gracefully degrades; results stay correct.
-- ==========================================================================

DROP TABLE IF EXISTS t_u32;
DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_i32;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_str2;
DROP TABLE IF EXISTS t_str3;

CREATE TABLE t_u32  (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_u64  (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_i32  (x Int32)  ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_str  (x String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_str2 (x String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_str3 (x String STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;

INSERT INTO t_u32  SELECT number FROM numbers(5);
INSERT INTO t_u64  SELECT number FROM numbers(5);
INSERT INTO t_i32  SELECT number FROM numbers(5);
INSERT INTO t_str  SELECT toString(number) FROM numbers(5);
INSERT INTO t_str2 SELECT toString(number) FROM numbers(5);
INSERT INTO t_str3 SELECT toString(number) FROM numbers(5);

-- 13a: mixed integer widths — casts prevent equivalences, no transitive synthesis
SELECT '-- Case 13a: mixed integer widths';
SELECT t_u32.x, t_u64.x, t_i32.x
FROM t_u32, t_u64, t_i32
WHERE t_u32.x = t_u64.x AND t_u64.x = t_i32.x
ORDER BY t_u32.x;

SELECT t_u32.x, t_u64.x, t_i32.x
FROM t_u32, t_u64, t_i32
WHERE t_u32.x = t_u64.x AND t_u64.x = t_i32.x
ORDER BY t_u32.x
SETTINGS enable_join_transitive_predicates = 0;

-- Plan: clauses contain _CAST, no transitive predicate synthesized
SELECT '-- Case 13a: plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_u32, t_u64, t_i32
    WHERE t_u32.x = t_u64.x AND t_u64.x = t_i32.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_join_swap_table = 'false'
) WHERE explain LIKE '%Clauses%';

-- 13b: same-type Strings — transitivity works, each step has a clause
SELECT '-- Case 13b: same-type Strings';
SELECT t_str.x, t_str2.x, t_str3.x
FROM t_str, t_str2, t_str3
WHERE t_str.x = t_str2.x AND t_str2.x = t_str3.x
ORDER BY t_str.x;

SELECT t_str.x, t_str2.x, t_str3.x
FROM t_str, t_str2, t_str3
WHERE t_str.x = t_str2.x AND t_str2.x = t_str3.x
ORDER BY t_str.x
SETTINGS enable_join_transitive_predicates = 0;

-- 13b plan: bias sizes so optimizer pairs t_str+t_str3 first, forcing synthesized predicate.
-- With column statistics (NDV via uniq), selectivity = 1/NDV for all pairs and cost is purely
-- proportional to |L|*|R|. Keeping t_str and t_str3 small while making t_str2 large guarantees
-- that the transitive pair (t_str, t_str3) has the lowest cost regardless of FROM clause order.
INSERT INTO t_str2 SELECT toString(number % 5) FROM numbers(10000);
SELECT '-- Case 13b: plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_str, t_str2, t_str3
    WHERE t_str.x = t_str2.x AND t_str2.x = t_str3.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
             query_plan_join_swap_table = 'false',
             use_statistics = 1
) WHERE explain LIKE '%Clauses%';

DROP TABLE t_u32;
DROP TABLE t_u64;
DROP TABLE t_i32;
DROP TABLE t_str;
DROP TABLE t_str2;
DROP TABLE t_str3;

-- ==========================================================================
-- Case 14: Nullable and LowCardinality types
-- ==========================================================================

DROP TABLE IF EXISTS n1;
DROP TABLE IF EXISTS n2;
DROP TABLE IF EXISTS n3;
DROP TABLE IF EXISTS lc1;
DROP TABLE IF EXISTS lc2;
DROP TABLE IF EXISTS lc3;
DROP TABLE IF EXISTS nlc1;
DROP TABLE IF EXISTS nu2;
DROP TABLE IF EXISTS nlc3;

-- 14a: Nullable(UInt32) all same — transitivity works
CREATE TABLE n1 (x Nullable(UInt32) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE n2 (x Nullable(UInt32) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE n3 (x Nullable(UInt32) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO n1 SELECT if(number % 3 = 0, NULL, number) FROM numbers(6);
INSERT INTO n2 SELECT if(number % 4 = 0, NULL, number) FROM numbers(6);
INSERT INTO n3 SELECT number FROM numbers(6);

SELECT '-- Case 14a: Nullable same type';
SELECT n1.x, n2.x, n3.x FROM n1, n2, n3
WHERE n1.x = n2.x AND n2.x = n3.x ORDER BY n1.x;

SELECT n1.x, n2.x, n3.x FROM n1, n2, n3
WHERE n1.x = n2.x AND n2.x = n3.x ORDER BY n1.x
SETTINGS enable_join_transitive_predicates = 0;

-- 14a plan: with column statistics, cost(n1,n3) << cost(n1,n2) and cost(n2,n3).
INSERT INTO n2 SELECT number % 6 FROM numbers(10000);
SELECT '-- Case 14a: plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM n1, n2, n3 WHERE n1.x = n2.x AND n2.x = n3.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
             query_plan_join_swap_table = 'false',
             use_statistics = 1
) WHERE explain LIKE '%Clauses%';

DROP TABLE n1; DROP TABLE n2; DROP TABLE n3;

-- 14b: Nullable vs non-Nullable — casts prevent equivalences
CREATE TABLE nlc1 (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE nu2  (x UInt32)           ENGINE = MergeTree ORDER BY x;
CREATE TABLE nlc3 (x Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nlc1 SELECT number FROM numbers(5);
INSERT INTO nu2  SELECT number FROM numbers(5);
INSERT INTO nlc3 SELECT number FROM numbers(5);

SELECT '-- Case 14b: Nullable vs non-Nullable';
SELECT nlc1.x, nu2.x, nlc3.x FROM nlc1, nu2, nlc3
WHERE nlc1.x = nu2.x AND nu2.x = nlc3.x ORDER BY nlc1.x;

SELECT nlc1.x, nu2.x, nlc3.x FROM nlc1, nu2, nlc3
WHERE nlc1.x = nu2.x AND nu2.x = nlc3.x ORDER BY nlc1.x
SETTINGS enable_join_transitive_predicates = 0;

SELECT '-- Case 14b: plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM nlc1, nu2, nlc3 WHERE nlc1.x = nu2.x AND nu2.x = nlc3.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_join_swap_table = 'false'
) WHERE explain LIKE '%Clauses%';

DROP TABLE nlc1; DROP TABLE nu2; DROP TABLE nlc3;

-- 14c: LowCardinality(String) all same — transitivity works
CREATE TABLE lc1 (x LowCardinality(String) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE lc2 (x LowCardinality(String) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;
CREATE TABLE lc3 (x LowCardinality(String) STATISTICS(uniq)) ENGINE = MergeTree ORDER BY x;
INSERT INTO lc1 SELECT toString(number) FROM numbers(5);
INSERT INTO lc2 SELECT toString(number) FROM numbers(5);
INSERT INTO lc3 SELECT toString(number) FROM numbers(5);

SELECT '-- Case 14c: LowCardinality same type';
SELECT lc1.x, lc2.x, lc3.x FROM lc1, lc2, lc3
WHERE lc1.x = lc2.x AND lc2.x = lc3.x ORDER BY lc1.x;

SELECT lc1.x, lc2.x, lc3.x FROM lc1, lc2, lc3
WHERE lc1.x = lc2.x AND lc2.x = lc3.x ORDER BY lc1.x
SETTINGS enable_join_transitive_predicates = 0;

-- 14c plan: with column statistics, cost(lc1,lc3) << cost(lc1,lc2) and cost(lc2,lc3).
INSERT INTO lc2 SELECT toString(number % 5) FROM numbers(10000);
SELECT '-- Case 14c: plan';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT count() FROM lc1, lc2, lc3 WHERE lc1.x = lc2.x AND lc2.x = lc3.x
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize',
             query_plan_join_swap_table = 'false',
             use_statistics = 1
) WHERE explain LIKE '%Clauses%';

DROP TABLE lc1; DROP TABLE lc2; DROP TABLE lc3;

DROP TABLE e1;
DROP TABLE e2;
DROP TABLE e3;
