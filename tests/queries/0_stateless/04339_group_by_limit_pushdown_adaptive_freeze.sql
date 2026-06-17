-- The top-K heap freezes once it has observed many rows while full but never skipped
-- or evicted (distinct-key count == LIMIT). Results must match the non-optimized plan,
-- including when smaller keys arrive after the freeze, and the freeze path must run.

SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'Pattern 1 freeze then smaller keys: result matches non-optimized';
-- First 400K rows fill the heap with exactly LIMIT (3) keys so it freezes; later rows
-- introduce smaller keys that are the true top-3, which a frozen heap must not drop.
SELECT count() FROM
(
    SELECT k, count() AS c FROM (SELECT if(number < 400000, 1000 + number % 3, number % 100)::UInt32 AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
INNER JOIN
(
    SELECT k, count() AS c FROM (SELECT if(number < 400000, 1000 + number % 3, number % 100)::UInt32 AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (k, c);

SELECT 'Pattern 2 freeze (cardinality == LIMIT): every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT (number % 5)::UInt32 AS k, 1 AS v FROM numbers(600000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT (number % 5)::UInt32 AS k, 1 AS v FROM numbers(600000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT 'Composite Pattern 2 freeze (cardinality == LIMIT, q3 shape)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (a, b)
);

SELECT 'Eviction-heavy stream must not freeze (results stay top-K correct)';
SELECT k, count() FROM (SELECT toUInt32(999999 - number) % 1000000 AS k FROM numbers(1000000)) GROUP BY k ORDER BY k ASC LIMIT 3;

SELECT 'freeze path ran';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['AggregationTopKHeapsFrozen']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';
