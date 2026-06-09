-- Tests for issue #92834: `Logical error: 'Part minmax index by time is neither
-- DateTime or DateTime64'` thrown from `getMinMaxTime` when querying
-- `system.parts` after certain `ALTER` / `Nullable` partition-key combinations.
--
-- Three independent paths can land on a `hyperrectangle` slot whose `Field`
-- type is not `UInt64` (DateTime) or `Decimal64` (DateTime64):
--   1. `ALTER TABLE ... MODIFY COLUMN ... AFTER` reorders a partition-key
--      column. `storage.minmax_idx_time_column_pos` (cached at table
--      creation/attach) is not refreshed, while subsequent `INSERT`s build the
--      per-part `hyperrectangle` in the *new* order. The cached pos now points
--      at a column of some other type (e.g. `Nullable(Int8)` → `Int64` `Field`).
--   2. A `Nullable(Date/DateTime/DateTime64)` partition-key column with every
--      row `NULL`: `ColumnNullable::getExtremesNullLast` returns
--      `POSITIVE_INFINITY` for both bounds (`Field::Types::Null`).
--   3. The same column type with mixed `NULL` / non-`NULL` rows: only the
--      upper bound becomes `POSITIVE_INFINITY` because `NULL` sorts last.
--
-- The fix:
--   * `getMinMaxDate` / `getMinMaxTime` short-circuit on `Field::Types::Null`
--     for either bound, and (in `getMinMaxTime`) return an empty range for any
--     unexpected `Field` type instead of throwing `LOGICAL_ERROR`. This makes
--     `system.parts` queries succeed (showing epoch / `1970-01-01` for those
--     parts) instead of failing.
--   * `checkPartitionKeyAndInitMinMax` unwraps `Nullable` before the
--     `isDate` / `isDateTime` / `isDateTime64` checks, so a non-`NULL`
--     `Nullable(...)` partition key actually populates `min_*`/`max_*` instead
--     of staying silently empty.
--
-- The deeper root cause of (1) — stale `minmax_idx_*_column_pos` after `ALTER
-- ... AFTER` — is not addressed here; a concurrency-safe fix needs per-part
-- column-order persistence and is left for a follow-up.

-- =====================================================
-- Case 1: The exact reproducer from issue #92834 (path 1 above).
-- `Enum NULL` + `DateTime` in the partition key, then
-- `ALTER MODIFY COLUMN ... AFTER` reorders the columns, then `INSERT`. Reading
-- `system.parts` previously threw `Part minmax index by time is neither
-- DateTime or DateTime64`. With the fix, the query must complete.
-- =====================================================
DROP TABLE IF EXISTS issue_92834_repro;

CREATE TABLE issue_92834_repro (c1 Enum('a' = 1) NULL, c2 DateTime) ENGINE = MergeTree()
    PARTITION BY (c1, c2) ORDER BY tuple() SETTINGS allow_nullable_key = 1;

ALTER TABLE issue_92834_repro MODIFY COLUMN c1 Nullable(Int8) AFTER c2;

INSERT INTO TABLE issue_92834_repro (c1) VALUES (1);

SELECT 1 FROM system.parts WHERE database = currentDatabase() AND table = 'issue_92834_repro' AND active;

DROP TABLE IF EXISTS issue_92834_repro;

-- =====================================================
-- Case 2: Direct Nullable(DateTime) partition key with every row NULL.
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
-- Case 3: Nullable(DateTime64) all-NULL variant.
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
-- Case 4: Nullable(Date) partition key with every row NULL.
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
-- Case 5: Nullable(DateTime) partition key with a real non-NULL value.
-- Verifies that `removeNullable` is in effect so `minmax_idx_time_column_pos`
-- gets set and `system.parts.min_time` / `max_time` actually reflect the
-- value (rather than staying silently at 0 because pos stayed -1).
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
-- Case 6: Nullable(DateTime64) partition key with a real non-NULL value.
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
-- Case 7: Nullable(Date) partition key with a real non-NULL value.
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

-- =====================================================
-- Case 8: Mixed `NULL` / non-`NULL` rows in a single part — `Nullable(DateTime)`.
--
-- For `Nullable` partition-key columns, `ColumnNullable::getExtremesNullLast`
-- returns `POSITIVE_INFINITY` (`Field::Types::Null`) for the *upper* bound when
-- the part contains at least one `NULL` row alongside non-`NULL` rows
-- (NullLast convention: `NULL` sorts last). Without checking
-- `right.isNull()` in addition to `left.isNull()`, `getMinMaxDate` /
-- `getMinMaxTime` would call `safeGet<UInt64>` on a `Null` field and throw
-- `BAD_GET`. This case exercises the mixed-bound path explicitly.
--
-- We use `PARTITION BY coalesce(event_time, ...)` so that both `NULL` and
-- non-`NULL` rows share the same partition id and end up in a single part
-- whose minmax hyperrectangle has `left` = real value, `right` = `Null`.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime_mixed_part;

CREATE TABLE test_nullable_datetime_mixed_part (id UInt64, event_time Nullable(DateTime('UTC')))
ENGINE = MergeTree()
PARTITION BY coalesce(event_time, toDateTime('1970-01-01', 'UTC'))
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime_mixed_part VALUES (1, toDateTime('1970-01-01', 'UTC')), (2, NULL);

-- Reading min_time/max_time must not throw. With the right.isNull() guard
-- the mixed-bound part returns the empty range and surfaces as epoch (0).
SELECT toUInt32(min_time) AS min_epoch, toUInt32(max_time) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime_mixed_part' AND active;

DROP TABLE IF EXISTS test_nullable_datetime_mixed_part;

-- =====================================================
-- Case 9: Mixed `NULL` / non-`NULL` rows in a single part — `Nullable(Date)`.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_date_mixed_part;

CREATE TABLE test_nullable_date_mixed_part (id UInt64, event_date Nullable(Date))
ENGINE = MergeTree()
PARTITION BY coalesce(event_date, toDate('1970-01-01'))
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_date_mixed_part VALUES (1, toDate('1970-01-01')), (2, NULL);

SELECT toUInt32(min_date) AS min_epoch, toUInt32(max_date) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_date_mixed_part' AND active;

DROP TABLE IF EXISTS test_nullable_date_mixed_part;

-- =====================================================
-- Case 10: Mixed `NULL` / non-`NULL` rows in a single part — `Nullable(DateTime64)`.
-- =====================================================
DROP TABLE IF EXISTS test_nullable_datetime64_mixed_part;

CREATE TABLE test_nullable_datetime64_mixed_part (id UInt64, event_time Nullable(DateTime64(3, 'UTC')))
ENGINE = MergeTree()
PARTITION BY coalesce(event_time, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC'))
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_nullable_datetime64_mixed_part VALUES (1, toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC')), (2, NULL);

SELECT toUInt32(min_time) AS min_epoch, toUInt32(max_time) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_nullable_datetime64_mixed_part' AND active;

DROP TABLE IF EXISTS test_nullable_datetime64_mixed_part;
