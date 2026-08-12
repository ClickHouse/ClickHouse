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

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_wrapped_key WHERE ts >= '2026-04-01 00:00:00') WHERE explain LIKE '%Condition:%' OR explain LIKE '%Granules:%';

-- Constants outside the UInt32 range cannot be pushed through toUnixTimestamp.
-- The queries must return correct results without an exception.
SELECT count() FROM test_wrapped_key WHERE ts >= '2110-01-01 00:00:00';
SELECT count() FROM test_wrapped_key WHERE ts >= '1960-01-01 00:00:00';
SELECT count() FROM test_wrapped_key WHERE ts < '1960-01-01 00:00:00';

DROP TABLE test_wrapped_key;

-- The same must work for other integer conversions of DateTime64, e.g. toInt64.

DROP TABLE IF EXISTS test_wrapped_key_int64;
CREATE TABLE test_wrapped_key_int64 (ts DateTime64(3))
ENGINE = MergeTree ORDER BY toInt64(ts) SETTINGS index_granularity = 1;

INSERT INTO test_wrapped_key_int64 SELECT '2026-03-01 00:00:00.000' FROM numbers(3);
INSERT INTO test_wrapped_key_int64 SELECT '2026-06-01 00:00:00.999' FROM numbers(3);

OPTIMIZE TABLE test_wrapped_key_int64 FINAL;

SELECT count() FROM test_wrapped_key_int64 WHERE ts >= '2026-04-01 00:00:00' SETTINGS force_primary_key = 1;

DROP TABLE test_wrapped_key_int64;

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

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_raw_key WHERE toUnixTimestamp(ts) >= 1768867200) WHERE explain LIKE '%Condition:%';

DROP TABLE test_raw_key;
