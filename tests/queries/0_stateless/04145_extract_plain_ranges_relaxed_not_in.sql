-- Regression for https://github.com/ClickHouse/ClickHouse/issues/103660
-- `KeyCondition::extractPlainRanges` must not treat a relaxed (deduplicated/transformed) set
-- as exact. For `tuple(i, i) NOT IN (tuple(1, 2))` `MergeTreeSetIndex` deduplicates the tuple
-- columns to a single key column, producing the relaxed set `{1}`. Building the complement
-- `(-inf, 1) U (1, +inf)` and treating it as exact ranges incorrectly skips `i = 1`,
-- even though `tuple(1, 1) != tuple(1, 2)`.

-- `numbers()` consumes the extracted plain ranges directly, so the bug surfaces here.
SELECT 'numbers NOT IN tuple';
SELECT number FROM numbers(5) WHERE tuple(number, number) NOT IN (tuple(1, 2)) ORDER BY number;

-- The `NOT has` variant goes through the same code path via `tryPrepareSetIndexForHas`.
SELECT 'numbers NOT has tuple';
SELECT number FROM numbers(5) WHERE NOT has([tuple(1, 2)], (number, number)) ORDER BY number;

-- Per-row evaluation sanity check: every `tuple(n, n)` differs from `tuple(1, 2)`,
-- so `NOT IN` is `1` for every row.
SELECT 'per-row eval';
SELECT number, tuple(number, number) NOT IN (tuple(1, 2)) AS val FROM numbers(5) ORDER BY number;

-- The mirror `IN` predicate must remain consistent: no row has `tuple(n, n) = tuple(1, 2)`.
SELECT 'numbers IN tuple';
SELECT number FROM numbers(5) WHERE tuple(number, number) IN (tuple(1, 2)) ORDER BY number;

-- `generate_series` shares the same range-honoring source. Its column is named `generate_series`.
SELECT 'generate_series NOT IN tuple';
SELECT generate_series FROM generate_series(0, 4) WHERE tuple(generate_series, generate_series) NOT IN (tuple(1, 2)) ORDER BY generate_series;

-- Sanity: a real `MergeTree` table with a relaxed predicate must also return all rows.
DROP TABLE IF EXISTS t_extract_plain_ranges_relaxed;
CREATE TABLE t_extract_plain_ranges_relaxed (i UInt64) ENGINE = MergeTree ORDER BY i;
INSERT INTO t_extract_plain_ranges_relaxed SELECT number FROM numbers(5);

SELECT 'mergetree NOT IN tuple';
SELECT i FROM t_extract_plain_ranges_relaxed WHERE tuple(i, i) NOT IN (tuple(1, 2)) ORDER BY i;

DROP TABLE t_extract_plain_ranges_relaxed;

-- `system.primes` consumes the same extracted ranges via `NumbersLikeUtils::extractRanges`.
-- Without the fix, the relaxed set `{2}` would build the exact range `(-inf, 2) U (2, +inf)`,
-- causing the source to skip the prime `2`.
SELECT 'primes NOT IN tuple';
SELECT prime FROM system.primes WHERE prime < 20 AND tuple(prime, prime) NOT IN (tuple(2, 3)) ORDER BY prime;

-- The mirror `IN` predicate must remain consistent: no prime has `tuple(p, p) = tuple(2, 3)`.
SELECT 'primes IN tuple';
SELECT prime FROM system.primes WHERE prime < 20 AND tuple(prime, prime) IN (tuple(2, 3)) ORDER BY prime;

-- `LIMIT` pushdown over exact ranges is the most user-visible failure mode: the source stops
-- early, so even a small `LIMIT` reveals the missing row. With the fix, ranges are conservative
-- and the row-level filter ensures the predicate is evaluated correctly before the `LIMIT` cuts in.
SELECT 'numbers NOT IN tuple LIMIT';
SELECT number FROM numbers(100) WHERE tuple(number, number) NOT IN (tuple(1, 2)) ORDER BY number LIMIT 3;

-- Combined `system.primes` + `LIMIT` regression: without the fix the source would skip the prime
-- `2`, returning `3, 5, 7, 11, 13` instead of `2, 3, 5, 7, 11`.
SELECT 'primes NOT IN tuple LIMIT';
SELECT prime FROM system.primes WHERE prime < 100 AND tuple(prime, prime) NOT IN (tuple(2, 3)) ORDER BY prime LIMIT 5;
