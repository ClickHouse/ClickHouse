-- { echo }

SET parallel_replicas_local_plan = 1;
SET session_timezone = 'UTC';

-- A filter on the raw DateTime64 column must be pushed through the monotonic
-- toUnixTimestamp wrapper in the sorting key (issue #114407, a regression in 26.7).

DROP TABLE IF EXISTS test_wrapped_key;
CREATE TABLE test_wrapped_key (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toUnixTimestamp(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key SELECT '2026-03-01 00:00:00.000' FROM numbers(3);
INSERT INTO test_wrapped_key SELECT '2026-04-15 12:00:00.500' FROM numbers(3);
INSERT INTO test_wrapped_key SELECT '2026-06-01 00:00:00.999' FROM numbers(3);

OPTIMIZE TABLE test_wrapped_key FINAL;

SELECT count() FROM test_wrapped_key WHERE ts >= '2026-04-01 00:00:00' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key WHERE ts < '2026-04-01 00:00:00' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key WHERE ts = '2026-04-15 12:00:00.500' SETTINGS force_primary_key = 1;

-- The relaxed atom truncates sub-second bounds; it may over-read but must not lose rows.
SELECT count() FROM test_wrapped_key WHERE ts > '2026-04-15 12:00:00.400' AND ts < '2026-04-15 12:00:00.600' SETTINGS force_primary_key = 1;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM test_wrapped_key WHERE ts >= '2026-04-01 00:00:00') WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

-- Constants outside the UInt32 range cannot be pushed through toUnixTimestamp.
-- The queries must return correct results without an exception.
SELECT count() FROM test_wrapped_key WHERE ts >= '2110-01-01 00:00:00';
SELECT count() FROM test_wrapped_key WHERE ts >= '1960-01-01 00:00:00';
SELECT count() FROM test_wrapped_key WHERE ts < '1960-01-01 00:00:00';

DROP TABLE test_wrapped_key;

-- Nullable DateTime64 keys have the same monotonic conversion. NULL values sort last,
-- so they can be safe false positives for a non-NULL range predicate but must not disable pruning.

DROP TABLE IF EXISTS test_wrapped_nullable_key;
CREATE TABLE test_wrapped_nullable_key (ts Nullable(DateTime64(3)))
ENGINE = MergeTree ORDER BY toUnixTimestamp(ts) SETTINGS allow_nullable_key = 1, index_granularity = 1;

INSERT INTO test_wrapped_nullable_key VALUES
    ('2026-03-01 00:00:00.000'),
    ('2026-04-15 12:00:00.500'),
    ('2026-06-01 00:00:00.999'),
    (NULL);

OPTIMIZE TABLE test_wrapped_nullable_key FINAL;

SELECT count() FROM test_wrapped_nullable_key WHERE ts >= '2026-04-01 00:00:00' SETTINGS force_primary_key = 1;
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM test_wrapped_nullable_key WHERE ts >= '2026-04-01 00:00:00') WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

DROP TABLE test_wrapped_nullable_key;

-- The same must work for other integer conversions of DateTime64, e.g. toInt64.

DROP TABLE IF EXISTS test_wrapped_key_int64;
CREATE TABLE test_wrapped_key_int64 (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toInt64(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key_int64 SELECT '2026-03-01 00:00:00.000' FROM numbers(3);
INSERT INTO test_wrapped_key_int64 SELECT '2026-06-01 00:00:00.999' FROM numbers(3);

OPTIMIZE TABLE test_wrapped_key_int64 FINAL;

SELECT count() FROM test_wrapped_key_int64 WHERE ts >= '2026-04-01 00:00:00' SETTINGS force_primary_key = 1;

DROP TABLE test_wrapped_key_int64;

-- A signed integer target is defined for negative timestamps as well,
-- so pruning must work for pre-1970 filters too.

DROP TABLE IF EXISTS test_wrapped_key_signed;
CREATE TABLE test_wrapped_key_signed (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toInt128(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key_signed SELECT '1969-06-01 00:00:00.000' FROM numbers(3);
INSERT INTO test_wrapped_key_signed SELECT '1969-12-31 23:59:59.500' FROM numbers(3);
INSERT INTO test_wrapped_key_signed SELECT '1970-06-01 00:00:00.999' FROM numbers(3);

OPTIMIZE TABLE test_wrapped_key_signed FINAL;

SELECT count() FROM test_wrapped_key_signed WHERE ts >= '1969-12-31 23:59:00' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key_signed WHERE ts < '1969-12-31 23:59:00' SETTINGS force_primary_key = 1;

-- The relaxed atom truncates negative sub-second bounds toward zero; it may over-read but must not lose rows.
SELECT count() FROM test_wrapped_key_signed WHERE ts > '1969-12-31 23:59:59.400' AND ts < '1970-01-01 00:00:00.100' SETTINGS force_primary_key = 1;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM test_wrapped_key_signed WHERE ts >= '1969-12-31 23:59:00') WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

DROP TABLE test_wrapped_key_signed;

-- A narrow integer target: a constant beyond the target range must be rejected
-- gracefully (no pruning), without a DECIMAL_OVERFLOW exception during analysis.

DROP TABLE IF EXISTS test_wrapped_key_uint8;
CREATE TABLE test_wrapped_key_uint8 (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toUInt8(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key_uint8 VALUES ('1970-01-01 00:01:00.000');

SELECT count() FROM test_wrapped_key_uint8 WHERE ts >= '1970-01-01 00:05:00';
SELECT count() FROM test_wrapped_key_uint8 WHERE ts < '1970-01-01 00:05:00';
SELECT count() FROM test_wrapped_key_uint8 WHERE ts >= '1969-01-01 00:00:00';

-- A constant within the target range still prunes.
SELECT count() FROM test_wrapped_key_uint8 WHERE ts >= '1970-01-01 00:00:30' SETTINGS force_primary_key = 1;

-- An unsigned target is rejected by the whole number of seconds, not by the raw tick value,
-- so a pre-epoch sub-second constant is inside the domain (it converts to 0) and still prunes.
SELECT count() FROM test_wrapped_key_uint8 WHERE ts >= '1969-12-31 23:59:59.500' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key_uint8 WHERE ts < '1969-12-31 23:59:59.500' SETTINGS force_primary_key = 1;

DROP TABLE test_wrapped_key_uint8;

-- The same for a wide unsigned integer key, with rows on both sides of the epoch boundary slice.

DROP TABLE IF EXISTS test_wrapped_key_uint128;
CREATE TABLE test_wrapped_key_uint128 (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toUInt128(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key_uint128 VALUES ('1969-12-31 23:59:59.500'), ('1970-01-01 00:00:00.500'), ('1970-01-01 00:01:00.000');

OPTIMIZE TABLE test_wrapped_key_uint128 FINAL;

SELECT count() FROM test_wrapped_key_uint128 WHERE ts >= '1969-12-31 23:59:59.500' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key_uint128 WHERE ts >= '1970-01-01 00:00:30' SETTINGS force_primary_key = 1;
SELECT count() FROM test_wrapped_key_uint128 WHERE ts < '1970-01-01 00:00:30' SETTINGS force_primary_key = 1;

DROP TABLE test_wrapped_key_uint128;

-- When the key is the raw DateTime64 column and the filter wraps it in toUnixTimestamp,
-- a part may contain values outside the UInt32 range next to values inside it.
-- Index analysis applies functions to whole columns of index values, so it must not
-- claim monotonicity over concrete ranges and must not throw an exception here.

DROP TABLE IF EXISTS test_raw_key;
CREATE TABLE test_raw_key (ts DateTime64(3))
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 4;

INSERT INTO test_raw_key SELECT toDateTime64('2026-01-01 00:00:00', 3) + toIntervalDay(number) FROM numbers(32);
INSERT INTO test_raw_key SELECT toDateTime64('2150-01-01 00:00:00', 3) + toIntervalDay(number) FROM numbers(32);

OPTIMIZE TABLE test_raw_key FINAL;

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM test_raw_key WHERE toUnixTimestamp(ts) >= 1768867200 SETTINGS optimize_use_implicit_projections = 1) WHERE explain LIKE '%Condition:%';

-- toInt64 of DateTime64 is total (the whole number of seconds always fits), so index
-- analysis may apply it to concrete ranges of index values and prune, even when the part
-- also contains values outside the UInt32 range.

SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT count() FROM test_raw_key WHERE toInt64(ts) >= toInt64(toDateTime64('2150-01-01 00:00:00', 3)) SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

-- The total conversion also allows the exact-ranges optimization of count(); the exact range
-- must agree with the per-granule analysis (checked by an assertion in debug builds).
SELECT count() FROM test_raw_key WHERE toInt64(ts) >= toInt64(toDateTime64('2150-01-01 00:00:00', 3)) SETTINGS force_primary_key = 1, optimize_use_implicit_projections = 1;

DROP TABLE test_raw_key;
