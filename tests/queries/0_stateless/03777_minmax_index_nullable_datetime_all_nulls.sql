-- Test for `system.parts.min_time` / `max_time` / `min_date` / `max_date`
-- reporting with `Nullable(Date/DateTime/DateTime64)` partition keys. (Found
-- while investigating issue #92834; see that issue for a separate, unrelated
-- stale-position bug after `ALTER MODIFY COLUMN ... AFTER` reorders a
-- partition-key column, which this PR does NOT address.)
--
-- Before this fix, `checkPartitionKeyAndInitMinMax` did not unwrap `Nullable`,
-- so `isDate` / `isDateTime` / `isDateTime64` returned false on a `Nullable(...)`
-- partition key and `minmax_idx_{date,time}_column_pos` was left at `-1`. As a
-- result `system.parts.min_*/max_*` silently returned `0` regardless of the
-- actual data in the part — the non-`NULL` mixed case was silently wrong.
--
-- Unwrapping `Nullable` fixes that, but the same change means that for a part
-- whose partition-key column is all-`NULL`, `hyperrectangle[pos].left` is now
-- `Field::Types::Null` (`POSITIVE_INFINITY`, NullLast). The existing type
-- checks in `getMinMaxDate`/`getMinMaxTime` would then throw
-- `"Part minmax index by time is neither DateTime or DateTime64"`, so
-- `getMinMaxDate`/`getMinMaxTime` also short-circuit on `Field::Types::Null`
-- and return an empty range. `system.parts.min_time`/`max_time` are
-- non-Nullable, so they surface as epoch (`0`) in the all-`NULL` case.

-- =====================================================
-- Case 1: Direct Nullable(DateTime) partition key with all NULLs.
-- Must not throw; min_time/max_time collapse to epoch.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime_all_nulls;

CREATE TABLE test_nullable_datetime_all_nulls (id UInt64, event_time Nullable(DateTime))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime_all_nulls (id, event_time) VALUES (1, NULL), (2, NULL);

SELECT toUInt32(min_time) AS min_epoch, toUInt32(max_time) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime_all_nulls' AND active;

DROP TABLE IF EXISTS test_nullable_datetime_all_nulls;

-- =====================================================
-- Case 2: Nullable(DateTime64) variant.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime64_all_nulls;

CREATE TABLE test_nullable_datetime64_all_nulls (id UInt64, event_time Nullable(DateTime64(3)))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime64_all_nulls (id, event_time) VALUES (1, NULL), (2, NULL);

SELECT toUInt32(min_time) AS min_epoch, toUInt32(max_time) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_all_nulls' AND active;

DROP TABLE IF EXISTS test_nullable_datetime64_all_nulls;

-- =====================================================
-- Case 3: Nullable(Date) partition key with all NULLs.
-- Covers the all-NULL branch in getMinMaxDate.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_date_all_nulls;

CREATE TABLE test_nullable_date_all_nulls (id UInt64, event_date Nullable(Date))
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_date_all_nulls (id, event_date) VALUES (1, NULL), (2, NULL);

SELECT toUInt32(min_date) AS min_epoch, toUInt32(max_date) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_date_all_nulls' AND active;

DROP TABLE IF EXISTS test_nullable_date_all_nulls;

-- =====================================================
-- Case 4: Nullable(DateTime) partition key with a real non-NULL value.
-- Verifies that `removeNullable` is in effect so minmax_idx_time_column_pos gets
-- set and system.parts.min_time / max_time actually reflect the value rather
-- than staying silently at 0 (which would happen if pos stayed -1).
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime_nonnull;

CREATE TABLE test_nullable_datetime_nonnull (id UInt64, event_time Nullable(DateTime))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime_nonnull (id, event_time) VALUES (1, toDateTime('2024-06-15 12:00:00', 'UTC'));

SELECT
    toUInt32(min_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS min_matches,
    toUInt32(max_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime_nonnull' AND active;

DROP TABLE IF EXISTS test_nullable_datetime_nonnull;

-- =====================================================
-- Case 5: Nullable(DateTime64) partition key with a real non-NULL value.
-- Directly covers `removeNullable` for DateTime64.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime64_nonnull;

CREATE TABLE test_nullable_datetime64_nonnull (id UInt64, event_time Nullable(DateTime64(3)))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime64_nonnull (id, event_time) VALUES (1, toDateTime64('2024-06-15 12:00:00.000', 3, 'UTC'));

SELECT
    toUInt32(min_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS min_matches,
    toUInt32(max_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_nonnull' AND active;

DROP TABLE IF EXISTS test_nullable_datetime64_nonnull;

-- =====================================================
-- Case 6: Nullable(Date) partition key with a real non-NULL value.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_date_nonnull;

CREATE TABLE test_nullable_date_nonnull (id UInt64, event_date Nullable(Date))
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_date_nonnull (id, event_date) VALUES (1, toDate('2024-06-15'));

SELECT
    min_date = toDate('2024-06-15') AS min_matches,
    max_date = toDate('2024-06-15') AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_date_nonnull' AND active;

DROP TABLE IF EXISTS test_nullable_date_nonnull;
