-- Reproducer for "Inconsistent KeyCondition behavior" with coalesce(const, key_column).
-- When the first argument of coalesce is a non-null constant,
-- matchesExactContinuousRange() incorrectly used types.front() (the constant's type)
-- instead of the actual varying argument type to check monotonicity.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS test_coalesce_reversed;

CREATE TABLE test_coalesce_reversed
(
    ts Nullable(Date)
)
ENGINE = MergeTree
ORDER BY ts
SETTINGS index_granularity = 1, allow_nullable_key = 1;

INSERT INTO test_coalesce_reversed VALUES
    ('2026-01-01'),
    ('2026-01-02'),
    (NULL),
    ('2026-01-03'),
    ('2026-01-04');

-- This query triggered "Inconsistent KeyCondition behavior" because
-- coalesce(non_null_const, ts) was treated as monotonic w.r.t. ts
-- due to using the wrong argument type in matchesExactContinuousRange().
SELECT count() FROM test_coalesce_reversed WHERE toDateTime64('2025-01-01 00:00:00', 3) > coalesce(toDateTime64('1970-01-01 00:00:00', 3), ts);

-- Also test the >= variant
SELECT count() FROM test_coalesce_reversed WHERE toDateTime64('2025-01-01 00:00:00', 3) >= coalesce(toDateTime64('1970-01-01 00:00:00', 3), ts);

-- And the normal order (coalesce(ts, const)) which should also work
SELECT count() FROM test_coalesce_reversed WHERE coalesce(ts, toDateTime64('1970-01-01 00:00:00', 3)) <= toDateTime64('2025-01-01 00:00:00', 3);

DROP TABLE test_coalesce_reversed;
