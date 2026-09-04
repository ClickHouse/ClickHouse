-- Coverage test: five guard conditions in convertJoinToIn that block join-to-IN conversion.
-- Each guard is a distinct early return in tryConvertJoinToIn
-- (src/Processors/QueryPlan/Optimizations/convertJoinToIn.cpp):
--   lines 152-154: non-hash join algorithm
--   lines 159-160: strictness is neither Any nor All (SEMI/ANTI/ASOF)
--   lines 163-164: non-INNER join kind (LEFT/RIGHT/FULL/CROSS)
--   lines 175-176: non-equality join condition (e.g. IS NOT DISTINCT FROM / null-safe equality)
--   lines 190-191: output columns come from the right-side table
-- Tags: no-random-settings, no-parallel-replicas

SET enable_analyzer = 1; -- targeted code runs only in the analyzer path; pin it so old-analyzer CI shards behave the same
SET query_plan_convert_join_to_in = 1;

-- Baseline: inner hash-join, equality condition, left-only output -> optimization MUST fire.
SELECT countIf(explain LIKE '%CreatingSets%') >= 1 AS baseline_converts
FROM (EXPLAIN description = 0
    SELECT t1.a FROM (SELECT 1::UInt64 AS a) t1 INNER JOIN (SELECT 1::UInt64 AS a) t2 ON t1.a = t2.a
    SETTINGS serialize_query_plan = 0);

-- Guard lines 152-154: partial_merge is not a hash algorithm -> optimization blocked.
SELECT countIf(explain LIKE '%CreatingSets%') = 0 AS guard_non_hash_algo
FROM (EXPLAIN description = 0
    SELECT t1.a FROM (SELECT 1::UInt64 AS a) t1 INNER JOIN (SELECT 1::UInt64 AS a) t2 ON t1.a = t2.a
    SETTINGS join_algorithm = 'partial_merge', serialize_query_plan = 0);

-- Guard lines 159-160: LEFT SEMI JOIN has strictness=semi (neither Any nor All) -> blocked.
SELECT countIf(explain LIKE '%CreatingSets%') = 0 AS guard_semi_strictness
FROM (EXPLAIN description = 0
    SELECT t1.a FROM (SELECT 1::UInt64 AS a) t1 LEFT SEMI JOIN (SELECT 1::UInt64 AS a) t2 ON t1.a = t2.a
    SETTINGS serialize_query_plan = 0);

-- Guard lines 163-164: LEFT JOIN has kind=left (not inner) -> blocked.
SELECT countIf(explain LIKE '%CreatingSets%') = 0 AS guard_left_kind
FROM (EXPLAIN description = 0
    SELECT t1.a FROM (SELECT 1::UInt64 AS a) t1 LEFT JOIN (SELECT 1::UInt64 AS a) t2 ON t1.a = t2.a
    SETTINGS serialize_query_plan = 0);

-- Guard lines 175-176: IS NOT DISTINCT FROM produces NullSafeEquals (op != Equals) -> blocked.
SELECT countIf(explain LIKE '%CreatingSets%') = 0 AS guard_null_safe_eq
FROM (EXPLAIN description = 0
    SELECT t1.a FROM (SELECT 1::UInt64 AS a) t1 INNER JOIN (SELECT 1::UInt64 AS a) t2 ON t1.a IS NOT DISTINCT FROM t2.a
    SETTINGS serialize_query_plan = 0);

-- Guard lines 190-191: SELECT includes a column from the right-side table -> blocked.
SELECT countIf(explain LIKE '%CreatingSets%') = 0 AS guard_right_output
FROM (EXPLAIN description = 0
    SELECT t1.a, t2.b FROM (SELECT 1::UInt64 AS a) t1 INNER JOIN (SELECT 1::UInt64 AS a, 2::UInt64 AS b) t2 ON t1.a = t2.a
    SETTINGS serialize_query_plan = 0);
