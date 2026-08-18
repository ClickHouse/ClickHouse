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
--     for either bound, and return an empty range for a bound whose `Field` kind
--     is *incompatible* with the expected one instead of throwing `LOGICAL_ERROR`
--     / `BAD_GET`. This makes `system.parts` queries succeed (showing epoch /
--     `1970-01-01` for those parts) instead of failing.
--   * `checkPartitionKeyAndInitMinMax` falls back to unwrapping `Nullable` and
--     `LowCardinality(Nullable(...))` (via `removeLowCardinalityAndNullable`)
--     only when the partition key has no non-`Nullable`
--     Date/DateTime/DateTime64 column, so an all-`Nullable` date/time key
--     populates `min_*`/`max_*` instead of staying silently empty, while a mixed
--     key such as `(d Date, nd Nullable(Date))` keeps selecting the non-`Nullable`
--     column `d` exactly as before.
--
-- Scope / not covered here:
--   * The deeper root cause of (1) — stale `minmax_idx_*_column_pos` after `ALTER
--     ... AFTER` — is not fixed; a concurrency-safe fix needs per-part
--     column-order persistence and is left for a follow-up.
--   * Consequently, only NULL bounds and *incompatible*-`Field` stale slots are
--     rescued. A stale slot pointing at a non-date column that shares the same
--     `Field` storage (a reordered `UInt64` read as `Date`/`DateTime`, or a plain
--     `Decimal64` read as `DateTime64`) is still read as a date/time and surfaces
--     a misleading value rather than an empty range — so this test does not assert
--     that such same-storage stale slots collapse to epoch.

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

-- =====================================================
-- Case 11: Mixed non-`Nullable` `Date` + `Nullable(Date)` in the partition key.
-- `checkPartitionKeyAndInitMinMax` must keep selecting the non-`Nullable` column
-- `d` (its historical choice) and NOT treat both `d` and `nd` as date candidates
-- (which would reset `minmax_idx_date_column_pos` to -1 and regress `min_date` /
-- `max_date` to epoch). `min_date` / `max_date` must therefore come from `d`
-- (2024-06-15), not from `nd` (2020-01-01) and not from epoch.
-- =====================================================
DROP TABLE IF EXISTS test_mixed_date_and_nullable_date;

CREATE TABLE test_mixed_date_and_nullable_date (id UInt64, d Date, nd Nullable(Date))
ENGINE = MergeTree()
PARTITION BY (d, nd)
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_mixed_date_and_nullable_date VALUES (1, toDate('2024-06-15'), toDate('2020-01-01'));

SELECT
    min_date = toDate('2024-06-15') AS min_matches,
    max_date = toDate('2024-06-15') AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_mixed_date_and_nullable_date' AND active;

DROP TABLE IF EXISTS test_mixed_date_and_nullable_date;

-- =====================================================
-- Case 12: Mixed non-`Nullable` `DateTime` + `Nullable(DateTime)` in the partition
-- key (the `DateTime` analogue of case 11). `min_time` / `max_time` must come from
-- the non-`Nullable` column `t` (2024-06-15 12:00:00), not epoch.
-- =====================================================
DROP TABLE IF EXISTS test_mixed_datetime_and_nullable_datetime;

CREATE TABLE test_mixed_datetime_and_nullable_datetime (id UInt64, t DateTime('UTC'), nt Nullable(DateTime('UTC')))
ENGINE = MergeTree()
PARTITION BY (t, nt)
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_mixed_datetime_and_nullable_datetime VALUES (1, toDateTime('2024-06-15 12:00:00', 'UTC'), toDateTime('2020-01-01 00:00:00', 'UTC'));

SELECT
    toUInt32(min_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS min_matches,
    toUInt32(max_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_mixed_datetime_and_nullable_datetime' AND active;

DROP TABLE IF EXISTS test_mixed_datetime_and_nullable_datetime;
-- =====================================================
-- Case 13: LowCardinality(Nullable(DateTime)) partition key with a real
-- non-NULL value. `allow_nullable_key` permits this wrapper too, and the minmax
-- writer unwraps `LowCardinality` before taking extremes, so the fallback in
-- `checkPartitionKeyAndInitMinMax` must treat it as a nullable date/time
-- carrier: `min_time` / `max_time` must reflect the value instead of staying
-- silently at epoch because `minmax_idx_time_column_pos` stayed -1.
-- =====================================================
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS test_lc_nullable_datetime_nonnull;

CREATE TABLE test_lc_nullable_datetime_nonnull (id UInt64, event_time LowCardinality(Nullable(DateTime('UTC'))))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_lc_nullable_datetime_nonnull (id, event_time) VALUES (1, toDateTime('2024-06-15 12:00:00', 'UTC'));

SELECT
    toUInt32(min_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS min_matches,
    toUInt32(max_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_lc_nullable_datetime_nonnull' AND active;

DROP TABLE IF EXISTS test_lc_nullable_datetime_nonnull;

-- =====================================================
-- Case 14: LowCardinality(Nullable(Date)) partition key with a real non-NULL
-- value. The `Date` analogue of case 13, covering the date branch of the
-- fallback.
-- =====================================================
DROP TABLE IF EXISTS test_lc_nullable_date_nonnull;

CREATE TABLE test_lc_nullable_date_nonnull (id UInt64, event_date LowCardinality(Nullable(Date)))
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_lc_nullable_date_nonnull (id, event_date) VALUES (1, toDate('2024-06-15'));

SELECT
    min_date = toDate('2024-06-15') AS min_matches,
    max_date = toDate('2024-06-15') AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_lc_nullable_date_nonnull' AND active;

DROP TABLE IF EXISTS test_lc_nullable_date_nonnull;

-- =====================================================
-- Case 15: LowCardinality(Nullable(DateTime)) partition key with every row
-- NULL. The minmax writer materializes the `LowCardinality` column and takes
-- `getExtremesNullLast`, so the bounds are `Null` exactly as in case 2 —
-- reading `system.parts` must not throw and the part surfaces as epoch.
-- =====================================================
DROP TABLE IF EXISTS test_lc_nullable_datetime_all_nulls;

-- =====================================================
-- Cases 16-17: Non-`Nullable` LowCardinality date/time partition keys.
-- `MinMaxIndex::update` materializes these columns before taking extremes, so
-- the preferred scan must unwrap `LowCardinality` too. Otherwise the actual
-- bounds are written, but `minmax_idx_*_column_pos` remains -1 and
-- `system.parts` reports epoch.
-- =====================================================
DROP TABLE IF EXISTS test_lc_datetime_nonnull;

CREATE TABLE test_lc_datetime_nonnull (id UInt64, event_time LowCardinality(DateTime('UTC')))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id;

INSERT INTO test_lc_datetime_nonnull (id, event_time) VALUES (1, toDateTime('2024-06-15 12:00:00', 'UTC'));

SELECT
    toUInt32(min_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS min_matches,
    toUInt32(max_time) = toUInt32(toDateTime('2024-06-15 12:00:00', 'UTC')) AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_lc_datetime_nonnull' AND active;

DROP TABLE IF EXISTS test_lc_datetime_nonnull;

DROP TABLE IF EXISTS test_lc_date_nonnull;

CREATE TABLE test_lc_date_nonnull (id UInt64, event_date LowCardinality(Date))
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY id;

INSERT INTO test_lc_date_nonnull (id, event_date) VALUES (1, toDate('2024-06-15'));

SELECT
    min_date = toDate('2024-06-15') AS min_matches,
    max_date = toDate('2024-06-15') AS max_matches
FROM system.parts WHERE database = currentDatabase() AND table = 'test_lc_date_nonnull' AND active;

DROP TABLE IF EXISTS test_lc_date_nonnull;

CREATE TABLE test_lc_nullable_datetime_all_nulls (id UInt64, event_time LowCardinality(Nullable(DateTime('UTC'))))
ENGINE = MergeTree()
PARTITION BY event_time
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_lc_nullable_datetime_all_nulls (id, event_time) VALUES (1, NULL), (2, NULL);

SELECT toUInt32(min_time) AS min_epoch, toUInt32(max_time) AS max_epoch
FROM system.parts WHERE database = currentDatabase() AND table = 'test_lc_nullable_datetime_all_nulls' AND active;

DROP TABLE IF EXISTS test_lc_nullable_datetime_all_nulls;
