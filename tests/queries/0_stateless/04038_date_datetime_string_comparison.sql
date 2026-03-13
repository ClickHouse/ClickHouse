-- Tests for comparing Date/Date32 with datetime string literals.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/99372
-- Previously, `today() < '2026-01-01 00:00:00'` failed with TYPE_MISMATCH because
-- the Date parser rejected the trailing time component.

-- Date vs datetime string (was failing before the fix)
SELECT today() < '2100-01-01 00:00:00';
SELECT today() > '1970-01-01 00:00:00';
SELECT today() = '2024-01-01 00:00:00';

-- Date vs date string (was already working)
SELECT today() < '2100-01-01';
SELECT today() > '1970-01-01';

-- DateTime vs datetime string (was already working)
SELECT now() < '2100-01-01 00:00:00';
SELECT now() > '1970-01-01 00:00:00';

-- DateTime vs date string (was already working)
SELECT now() < '2100-01-01';
SELECT now() > '1970-01-01';

-- Date32 vs datetime string (same bug, same fix)
SELECT toDate32('2024-01-01') < '2100-01-01 00:00:00';
SELECT toDate32('2024-01-01') > '1970-01-01 00:00:00';

-- The time part is correctly discarded (date comparison only)
SELECT toDate('2024-06-15') = '2024-06-15 23:59:59';  -- same day, should be 1
SELECT toDate('2024-06-15') = '2024-06-16 00:00:00';  -- different day, should be 0

-- INSERT with datetime string into Date column (also benefits from the fix)
CREATE TABLE test_date_insert (d Date) ENGINE = Memory;
INSERT INTO test_date_insert VALUES ('2024-01-15 12:34:56');
SELECT d FROM test_date_insert;
DROP TABLE test_date_insert;
