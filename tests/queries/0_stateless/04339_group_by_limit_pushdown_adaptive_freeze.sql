-- The top-K heap freezes itself when it has observed many rows without ever
-- skipping one or evicting a key (e.g. the number of distinct keys does not
-- exceed the LIMIT) - in that state every group is complete in the hash table
-- and the per-row boundary check is pure overhead.  These tests pin the
-- correctness of the freeze: results must match the non-optimized plan even
-- when the key cardinality grows after the heap froze.

SET enable_group_by_top_k_optimization = 1;
SET max_threads = 1;
-- The CI test profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

SELECT 'Pattern 1: cardinality below LIMIT for the freeze window, then new keys arrive';
-- First 500K rows cycle over 5 keys (the freeze threshold is 256K rows); the
-- rest introduce 100 new keys, some of which belong to the true top-3.
SELECT k, count() FROM
(
    SELECT if(number < 500000, toUInt32(number % 5 + 50), toUInt32(number % 100)) AS k
    FROM numbers(600000)
)
GROUP BY k ORDER BY k ASC LIMIT 3;

SELECT 'Pattern 1 result matches the non-optimized plan';
SELECT count() FROM
(
    SELECT k, count() AS c FROM (SELECT if(number < 500000, toUInt32(number % 5 + 50), toUInt32(number % 100)) AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
INNER JOIN
(
    SELECT k, count() AS c FROM (SELECT if(number < 500000, toUInt32(number % 5 + 50), toUInt32(number % 100)) AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (k, c);

SELECT 'Pattern 2: every returned group carries its complete aggregate after a freeze';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT toUInt32(number % 7) AS k, 1 AS v FROM numbers(600000)) GROUP BY k LIMIT 5 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT k, sum(v) AS s FROM (SELECT toUInt32(number % 7) AS k, 1 AS v FROM numbers(600000)) GROUP BY k SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (k)
);

SELECT 'Composite Pattern 2 with cardinality equal to LIMIT (perf-regression shape)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 1) AS l
    INNER JOIN (SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b SETTINGS enable_group_by_top_k_optimization = 0) AS f USING (a, b)
);

SELECT 'Eviction-heavy stream must not freeze (results stay top-K correct)';
SELECT k, count() FROM (SELECT toUInt32(999999 - number) % 1000000 AS k FROM numbers(1000000)) GROUP BY k ORDER BY k ASC LIMIT 3;
