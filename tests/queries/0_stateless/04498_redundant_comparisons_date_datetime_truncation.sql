-- Regression test for `optimize_redundant_comparisons` on `Date`/`Date32` columns compared with
-- `DateTime` constants.
--
-- The comparison-chain pruning converts comparison constants to the column type with a strict
-- (lossless) conversion and only folds/prunes when the conversion succeeds. However, strict
-- conversion did not reject the lossy `DateTime` -> `Date`/`Date32` truncation (`convertFieldToType`
-- silently drops the intra-day part), while the original comparison is evaluated in the wider
-- (`DateTime`) domain: a `Date` value promotes to midnight. So a `DateTime` constant with a non-zero
-- time-of-day collapsed onto the day and the optimizer folded the chain using a value different from
-- the original constant, changing query results compared to running with the optimization disabled.
--
-- Each scenario below must produce the same result with `optimize_redundant_comparisons` enabled
-- and disabled.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS 04498_date;
DROP TABLE IF EXISTS 04498_date32;

CREATE TABLE 04498_date (d Date) ENGINE = Memory;
INSERT INTO 04498_date VALUES ('2024-01-01'), ('2024-01-02');

CREATE TABLE 04498_date32 (d Date32) ENGINE = Memory;
INSERT INTO 04498_date32 VALUES ('2024-01-01'), ('2024-01-02');

-- equals + notEquals against a `DateTime` constant with a non-zero time-of-day: the row `2024-01-01`
-- promotes to `2024-01-01 00:00:00`, which differs from `2024-01-01 12:34:56`, so `d != ...` is true
-- and the row must be kept. The chain must not be folded to `false` using the truncated day value.
SELECT 'date_eq_neq_intraday';
SELECT count() FROM 04498_date
WHERE d = toDate('2024-01-01') AND d != toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04498_date
WHERE d = toDate('2024-01-01') AND d != toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 1;

-- equals + notEquals against a `DateTime` constant at midnight: this is a genuine contradiction
-- (`d = 2024-01-01 AND d != 2024-01-01 00:00:00`), so no rows match. The lossless case is still
-- optimized: the constant round-trips exactly, so the fold to `false` is allowed.
SELECT 'date_eq_neq_midnight';
SELECT count() FROM 04498_date
WHERE d = toDate('2024-01-01') AND d != toDateTime('2024-01-01 00:00:00')
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04498_date
WHERE d = toDate('2024-01-01') AND d != toDateTime('2024-01-01 00:00:00')
SETTINGS optimize_redundant_comparisons = 1;

-- notEquals + lessOrEquals against a `DateTime` constant with a non-zero time-of-day: for the row
-- `2024-01-01` both `d != 2024-01-01 12:34:56` and `d <= 2024-01-01 12:34:56` hold, so the row must
-- be kept; the `<=` must not be strengthened to `<` using the truncated day value.
SELECT 'date_neq_range_intraday';
SELECT count() FROM 04498_date
WHERE d != toDateTime('2024-01-01 12:34:56') AND d <= toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04498_date
WHERE d != toDateTime('2024-01-01 12:34:56') AND d <= toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 1;

-- Same intra-day case for a `Date32` column: the row `2024-01-01` must be kept.
SELECT 'date32_eq_neq_intraday';
SELECT count() FROM 04498_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04498_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime('2024-01-01 12:34:56')
SETTINGS optimize_redundant_comparisons = 1;

-- Midnight (lossless) case for a `Date32` column: a genuine contradiction, still optimized to no rows.
SELECT 'date32_eq_neq_midnight';
SELECT count() FROM 04498_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime('2024-01-01 00:00:00')
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04498_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime('2024-01-01 00:00:00')
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE 04498_date;
DROP TABLE 04498_date32;
