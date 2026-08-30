-- GROUP BY with uniqExact states large enough to be merged in parallel (deferred multi-way merge).
-- One heavily skewed key makes the per-thread partial sets cross the parallel-merge threshold.

-- Two-level result table, partial sets past the two-level threshold themselves.
SELECT k, uniqExact(n) AS u, count() AS c
FROM (SELECT if(number < 4800000, toUInt64(0), number % 16) AS k, number AS n FROM numbers_mt(6000000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 8;

-- Single-level result table, single-level partial sets: the merge converts them to two-level in parallel first.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- Variadic uniqExact goes through the same deferred path.
SELECT k, uniqExact(n, s) AS u
FROM (SELECT number % 2 AS k, number AS n, toString(number % 500000) AS s FROM numbers_mt(2000000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 16, group_by_two_level_threshold = 8;

-- Single thread: nothing is deferred, results must match.
SELECT k, uniqExact(n) AS u
FROM (SELECT if(number < 1200000, toUInt64(0), number % 4) AS k, number AS n FROM numbers_mt(1600000))
GROUP BY k ORDER BY k
SETTINGS max_threads = 1;
