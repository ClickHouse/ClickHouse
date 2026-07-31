-- Test: exercises `FunctionWithOptionalConstArg::getMonotonicityForRange` DateTime64 branch
-- when `toHour-like` function is invoked with a const timezone argument against a partitioned MergeTree.
-- Covers: src/Storages/MergeTree/KeyCondition.cpp:2577-2578 — the
--   `else if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(type_ptr))` branch
--   that constructs `DataTypeDateTime64(scale, time_zone)` so monotonicity is evaluated in the
--   user-supplied timezone, not the column's stored timezone.
-- The PR's own test (02346_to_hour_monotonicity_fix_2.sql) only exercises the `DataTypeDateTime`
-- branch (line 2575-2576). DateTime64 is an entirely separate typeid_cast.

DROP TABLE IF EXISTS test_dt64_tz;

CREATE TABLE test_dt64_tz (stamp DateTime64(3, 'UTC'))
ENGINE = MergeTree
PARTITION BY toDate(stamp)
ORDER BY tuple()
AS SELECT toDateTime64('2020-01-01', 3, 'UTC') + number*60 FROM numbers(1000);

SELECT count() FROM test_dt64_tz WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test_dt64_tz;

DROP TABLE IF EXISTS test_dt64_tz_nullable;

CREATE TABLE test_dt64_tz_nullable (stamp Nullable(DateTime64(3, 'UTC')))
ENGINE = MergeTree
PARTITION BY toDate(stamp)
ORDER BY tuple()
SETTINGS allow_nullable_key = 1
AS SELECT toDateTime64('2020-01-01', 3, 'UTC') + number*60 FROM numbers(1000);

SELECT count() FROM test_dt64_tz_nullable WHERE toHour(stamp, 'America/Montreal') = 7;

DROP TABLE test_dt64_tz_nullable;
