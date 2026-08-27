-- A `TTL ... DELETE WHERE` must be evaluated against the values a merge produces, not only against
-- the values the source rows were written with. The two value-combining modes whose rollup or
-- coalescing rules make the merge output hold a value no source row had.
--
-- Split across several tests of the same number so no single one runs long on the slower CI
-- configurations: `_combined_values`, `_ttl_merges_stopped`, `_patch_parts`, `_background`.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ttl_where_coalescing;

-- Coalescing takes the first non-NULL value per column, so a combination of `a` and `b` that no
-- single source row had can only appear in the merge output.
CREATE TABLE ttl_where_coalescing
(
    key UInt64,
    a Nullable(Int64),
    b Nullable(Int64),
    expiry DateTime
)
ENGINE = CoalescingMergeTree
ORDER BY (key, expiry)
TTL expiry DELETE WHERE a = 1 AND b = 2
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES ttl_where_coalescing;

INSERT INTO ttl_where_coalescing VALUES (1, 1, NULL, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (1, NULL, 2, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (2, 1, NULL, '2106-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (2, NULL, 2, '2106-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (3, 1, NULL, '2020-01-01 00:00:00');
INSERT INTO ttl_where_coalescing VALUES (3, NULL, 9, '2020-01-01 00:00:00');

SYSTEM START MERGES ttl_where_coalescing;
OPTIMIZE TABLE ttl_where_coalescing FINAL;

SELECT 'coalescing', key, a, b, expiry FROM ttl_where_coalescing ORDER BY key;

DROP TABLE ttl_where_coalescing;

-- `GraphiteMergeTree` rolls up the rows of a time bucket into one, so the rolled-up `Value` is a
-- value no source row had. `graphite_rollup` (from the stateless config) sums `Value` for a `Path`
-- matching `sum`, so two counters that cancel out roll up to zero. Rollup only aggregates rows that
-- arrive together in one block, so the pairs go in with a single INSERT - and with
-- `optimize_on_insert = 0`, because otherwise the INSERT itself rolls them up and applies the TTL,
-- and the merge this test is about never sees the pair.
DROP TABLE IF EXISTS ttl_where_graphite;

-- Rollup groups by `Path` and by `Time` rounded within the day, so `Path` separates the cases and
-- the two rows of a pair are 60 s apart - distinct rows to aggregate, one 6000 s bucket (their age
-- is past the pattern's 2-day boundary).
CREATE TABLE ttl_where_graphite
(
    Path String,
    Time DateTime('UTC'),
    Value Float64,
    Version UInt32,
    expiry DateTime
)
ENGINE = GraphiteMergeTree('graphite_rollup')
ORDER BY (Path, Time)
TTL expiry DELETE WHERE Value = 0
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO ttl_where_graphite SETTINGS optimize_on_insert = 0 VALUES
    -- sum_1: rolls up to Value = 0 and is expired -> must be deleted.
    ('sum_1', '2020-01-01 00:00:00', -1, 1, '2020-01-01 00:00:00'),
    ('sum_1', '2020-01-01 00:01:00', +1, 1, '2020-01-01 00:00:00'),
    -- sum_2: rolls up to Value = 0 but is not expired yet -> must survive.
    ('sum_2', '2020-01-01 00:00:00', -1, 1, '2106-01-01 00:00:00'),
    ('sum_2', '2020-01-01 00:01:00', +1, 1, '2106-01-01 00:00:00'),
    -- sum_3: expired but does not satisfy the WHERE -> must survive.
    ('sum_3', '2020-01-01 00:00:00', 5, 1, '2020-01-01 00:00:00');

OPTIMIZE TABLE ttl_where_graphite FINAL;

SELECT 'graphite', Path, Value, expiry FROM ttl_where_graphite ORDER BY Path;

DROP TABLE ttl_where_graphite;
