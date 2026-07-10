-- Tags: long
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
-- query_plan_remove_redundant_filter_conditions is on by default and is exercised as such

DROP TABLE IF EXISTS red_t;

-- b is the primary key; a and c are not covered by any index
CREATE TABLE red_t (a UInt64, b UInt64, c UInt64, payload String) ENGINE = MergeTree ORDER BY b;
INSERT INTO red_t SELECT number, number + 1, number + 2, toString(number) FROM numbers(1000000);

-- `optimize_and_compare_chain` (on by default) derives `b > 999000` here; `b` is the primary
-- key, so the derived condition prunes and must be kept (and reach the prewhere)
SELECT 'derived prunable kept',
       countIf(explain LIKE '%b > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < b
) WHERE explain LIKE '%Prewhere filter column%';

-- The same chain over non-indexed `c` derives `c > 999000`, which cannot prune anything:
-- the redundant per-row check must be removed again
SELECT 'derived non-prunable removed',
       countIf(explain LIKE '%c > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c
);

-- Hand-written redundant condition on a non-indexed column is removed too
SELECT 'hand-written removed',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c AND c > 999000
    SETTINGS optimize_and_compare_chain = 0
);

-- A looser hand-written bound is implied but not exactly derivable: it stays (only
-- exact transitive products are removed)
SELECT 'looser bound kept',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 500%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c AND c > 500
    SETTINGS optimize_and_compare_chain = 0
);

-- A tighter hand-written bound is NOT implied by the chain and must stay
SELECT 'tighter bound kept',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 999500%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c AND c > 999500
    SETTINGS optimize_and_compare_chain = 0
);

-- Equality conditions are never removed even when implied
SELECT 'equality kept',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c = 999500%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c AND c = 999500
    SETTINGS optimize_and_compare_chain = 0
);

-- Setting off: the redundant condition survives
SELECT 'setting off',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t
    WHERE 999000 < a AND a < c AND c > 999000
    SETTINGS optimize_and_compare_chain = 0, query_plan_remove_redundant_filter_conditions = 0
);

-- Join coverage for the target discovery: the derived condition lands on a join input
DROP TABLE IF EXISTS red_join_pk;
DROP TABLE IF EXISTS red_join_plain;
CREATE TABLE red_join_pk    (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE red_join_plain (k UInt64, payload String) ENGINE = MergeTree ORDER BY payload;
INSERT INTO red_join_pk    SELECT number, toString(number) FROM numbers(1000000);
INSERT INTO red_join_plain SELECT number, toString(number) FROM numbers(1000000);

-- The chain above the join derives `j.k > 999000`; `k` is the join table's primary key, so
-- the derived condition prunes there and must be kept
SELECT 'join prunable kept',
       countIf(explain LIKE '%ilter column:%k > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t AS t
    INNER JOIN red_join_pk AS j ON t.b = j.k
    WHERE 999000 < t.a AND t.a < j.k
);

-- Same shape onto a join table where `k` has no index: the derived condition is removed
SELECT 'join non-prunable removed',
       countIf(explain LIKE '%k > 999000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_t AS t
    INNER JOIN red_join_plain AS j ON t.b = j.k
    WHERE 999000 < t.a AND t.a < j.k
);

SELECT 'join prunable correctness',
       (SELECT count() FROM red_t AS t INNER JOIN red_join_pk AS j ON t.b = j.k
        WHERE 999000 < t.a AND t.a < j.k)
     - (SELECT count() FROM red_t AS t INNER JOIN red_join_pk AS j ON t.b = j.k
        WHERE 999000 < t.a AND t.a < j.k
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

SELECT 'join non-prunable correctness',
       (SELECT count() FROM red_t AS t INNER JOIN red_join_plain AS j ON t.b = j.k
        WHERE 999000 < t.a AND t.a < j.k)
     - (SELECT count() FROM red_t AS t INNER JOIN red_join_plain AS j ON t.b = j.k
        WHERE 999000 < t.a AND t.a < j.k
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- String domain: lexicographic chains behave like numeric ones
DROP TABLE IF EXISTS red_str_t;
CREATE TABLE red_str_t (sa String, sb String, sc String) ENGINE = MergeTree ORDER BY sb;
INSERT INTO red_str_t SELECT leftPad(toString(number), 9, '0'), leftPad(toString(number + 1), 9, '0'), leftPad(toString(number + 2), 9, '0') FROM numbers(100000);

SELECT 'string non-prunable removed',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%sc > \'000099000\'%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_str_t
    WHERE '000099000' < sa AND sa < sc AND sc > '000099000'
    SETTINGS optimize_and_compare_chain = 0
);

SELECT 'string correctness',
       (SELECT count() FROM red_str_t WHERE '000099000' < sa AND sa < sc AND sc > '000099000')
     - (SELECT count() FROM red_str_t WHERE '000099000' < sa AND sa < sc AND sc > '000099000'
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- Nullable numbers stay inside the numeric domain; NULL rows fail the remaining conjunction
-- exactly like they failed the removed condition
DROP TABLE IF EXISTS red_null_t;
CREATE TABLE red_null_t (a Nullable(UInt64), b UInt64, c Nullable(UInt64)) ENGINE = MergeTree ORDER BY b;
INSERT INTO red_null_t SELECT if(number % 10 = 0, NULL, number), number + 1, if(number % 7 = 0, NULL, number + 2) FROM numbers(100000);

SELECT 'nullable non-prunable removed',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 99000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_null_t
    WHERE 99000 < a AND a < c AND c > 99000
    SETTINGS optimize_and_compare_chain = 0
);

SELECT 'nullable correctness',
       (SELECT count() FROM red_null_t WHERE 99000 < a AND a < c AND c > 99000)
     - (SELECT count() FROM red_null_t WHERE 99000 < a AND a < c AND c > 99000
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- Floats with NaN: NaN rows fail every comparison, before and after the removal
DROP TABLE IF EXISTS red_float_t;
CREATE TABLE red_float_t (a Float64, b UInt64, c Float64) ENGINE = MergeTree ORDER BY b;
INSERT INTO red_float_t SELECT if(number % 10 = 0, nan, number), number + 1, if(number % 7 = 0, nan, number + 2) FROM numbers(100000);

SELECT 'float nan non-prunable removed',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%c > 99000%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_float_t
    WHERE 99000. < a AND a < c AND c > 99000.
    SETTINGS optimize_and_compare_chain = 0
);

SELECT 'float nan correctness',
       (SELECT count() FROM red_float_t WHERE 99000. < a AND a < c AND c > 99000.)
     - (SELECT count() FROM red_float_t WHERE 99000. < a AND a < c AND c > 99000.
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- Enum compares against strings by enum id, not lexicographically: such chains are outside
-- the proven comparison domain and nothing may be removed from them
DROP TABLE IF EXISTS red_enum;
CREATE TABLE red_enum (e Enum('a' = 5, 'z' = 1), e2 Enum('a' = 5, 'z' = 1), payload String)
ENGINE = MergeTree ORDER BY payload;
INSERT INTO red_enum SELECT 'a', 'a', toString(number) FROM numbers(1000);

SELECT 'enum domain kept',
       countIf(explain LIKE '%ilter column%' AND explain LIKE '%e2 >%')
FROM (
    EXPLAIN PLAN actions=1
    SELECT count()
    FROM red_enum
    WHERE 'z' < e AND e <= e2 AND e2 > 'z'
    SETTINGS optimize_and_compare_chain = 0
);

SELECT 'enum domain correctness',
       (SELECT count() FROM red_enum WHERE 'z' < e AND e <= e2 AND e2 > 'z')
     - (SELECT count() FROM red_enum WHERE 'z' < e AND e <= e2 AND e2 > 'z'
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- Relation cycle must not loop or over-remove
SELECT 'relation cycle result', count()
FROM red_t
WHERE a < b AND b < a AND 999000 < a;

-- Correctness: identical results with and without the removal
SELECT 'correctness',
       (SELECT count() FROM red_t WHERE 999000 < a AND a < c)
     - (SELECT count() FROM red_t WHERE 999000 < a AND a < c
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

SELECT 'hand-written correctness',
       (SELECT count() FROM red_t WHERE 999000 < a AND a < c AND c > 999000)
     - (SELECT count() FROM red_t WHERE 999000 < a AND a < c AND c > 999000
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

SELECT 'strictness correctness',
       (SELECT count() FROM red_t WHERE 999000 <= a AND a <= c AND c >= 999000)
     - (SELECT count() FROM red_t WHERE 999000 <= a AND a <= c AND c >= 999000
        SETTINGS query_plan_remove_redundant_filter_conditions = 0);

-- Result sanity: rows with a > 999000 (b = a + 1 > a, c = a + 2 > a always hold)
SELECT 'result', count() FROM red_t WHERE 999000 < a AND a < c;

DROP TABLE red_t;
DROP TABLE red_join_pk;
DROP TABLE red_join_plain;
DROP TABLE red_enum;
DROP TABLE red_str_t;
DROP TABLE red_null_t;
DROP TABLE red_float_t;
