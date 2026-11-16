-- Regression test for `optimize_redundant_comparisons` on the remaining lossy `strict` conversion
-- surfaces guarded by the round-trip reversibility check in `tryConvertToColumnType`.
--
-- The comparison-chain pruning converts comparison constants to the column type with a strict
-- (lossless) conversion and only folds/prunes when the conversion is exactly reversible. Several
-- value-narrowing conversions of a typed constant are lossy but were not rejected by
-- `convertFieldToType` alone, so the guard round-trips the converted value back and skips the
-- optimization when it does not match the original constant. `04489` covers `DateTime64` scale
-- reduction and `04498` covers `DateTime` -> `Date`/`Date32`; this test covers the sibling surfaces:
--   * `Time64` scale reduction,
--   * `DateTime64` -> `Date`/`Date32` truncation,
--   * `Float64` -> `Float32` narrowing.
--
-- Each scenario below must produce the same result with `optimize_redundant_comparisons` enabled
-- and disabled.

SET enable_analyzer = 1;
SET optimize_and_compare_chain = 1;

-- ---------------------------------------------------------------------------------------------------
-- Time64 scale reduction
-- ---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS 04499_time64;
CREATE TABLE 04499_time64 (t Time64(1)) ENGINE = Memory;
INSERT INTO 04499_time64 VALUES (toTime64('12:00:01.2', 1)), (toTime64('12:00:01.3', 1));

-- equals + notEquals against a finer-scale constant that is NOT representable in the column scale.
-- `1.2 != 1.23`, so the row `12:00:01.2` must be kept; the chain must not be folded to `false`.
SELECT 'time64_eq_neq_finer';
SELECT count() FROM 04499_time64
WHERE t = toTime64('12:00:01.2', 1) AND t != toTime64('12:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_time64
WHERE t = toTime64('12:00:01.2', 1) AND t != toTime64('12:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 1;

-- notEquals + lessOrEquals against a finer-scale constant: `1.2 != 1.23` and `1.2 <= 1.23`, so the
-- row `12:00:01.2` must be kept; the `<=` must not be strengthened to `<` using a truncated value.
SELECT 'time64_neq_range_finer';
SELECT count() FROM 04499_time64
WHERE t != toTime64('12:00:01.23', 2) AND t <= toTime64('12:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_time64
WHERE t != toTime64('12:00:01.23', 2) AND t <= toTime64('12:00:01.23', 2)
SETTINGS optimize_redundant_comparisons = 1;

-- A lossless finer-scale constant (`1.20` of scale 2 is exactly `1.2` of scale 1) is still optimized:
-- `t = 1.20 AND t != 1.20` is a genuine contradiction and yields no rows with or without the setting.
SELECT 'time64_eq_neq_lossless';
SELECT count() FROM 04499_time64
WHERE t = toTime64('12:00:01.20', 2) AND t != toTime64('12:00:01.20', 2)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_time64
WHERE t = toTime64('12:00:01.20', 2) AND t != toTime64('12:00:01.20', 2)
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE 04499_time64;

-- ---------------------------------------------------------------------------------------------------
-- DateTime64 -> Date / Date32 truncation
-- ---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS 04499_date;
DROP TABLE IF EXISTS 04499_date32;
CREATE TABLE 04499_date (d Date) ENGINE = Memory;
INSERT INTO 04499_date VALUES ('2024-01-01'), ('2024-01-02');
CREATE TABLE 04499_date32 (d Date32) ENGINE = Memory;
INSERT INTO 04499_date32 VALUES ('2024-01-01'), ('2024-01-02');

-- equals + notEquals against a `DateTime64` constant with a non-zero time-of-day: the row `2024-01-01`
-- promotes to `2024-01-01 00:00:00.000`, which differs from `2024-01-01 12:34:56.789`, so `d != ...`
-- is true and the row must be kept. The chain must not be folded to `false` using the truncated day.
SELECT 'date_eq_neq_dt64_intraday';
SELECT count() FROM 04499_date
WHERE d = toDate('2024-01-01') AND d != toDateTime64('2024-01-01 12:34:56.789', 3)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_date
WHERE d = toDate('2024-01-01') AND d != toDateTime64('2024-01-01 12:34:56.789', 3)
SETTINGS optimize_redundant_comparisons = 1;

-- equals + notEquals against a `DateTime64` constant at midnight with zero fractional part: this is a
-- genuine contradiction, so no rows match. The lossless case is still optimized (round-trips exactly).
SELECT 'date_eq_neq_dt64_midnight';
SELECT count() FROM 04499_date
WHERE d = toDate('2024-01-01') AND d != toDateTime64('2024-01-01 00:00:00.000', 3)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_date
WHERE d = toDate('2024-01-01') AND d != toDateTime64('2024-01-01 00:00:00.000', 3)
SETTINGS optimize_redundant_comparisons = 1;

-- Same intra-day case for a `Date32` column: the row `2024-01-01` must be kept.
SELECT 'date32_eq_neq_dt64_intraday';
SELECT count() FROM 04499_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime64('2024-01-01 12:34:56.789', 3)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime64('2024-01-01 12:34:56.789', 3)
SETTINGS optimize_redundant_comparisons = 1;

-- Midnight (lossless) case for a `Date32` column: a genuine contradiction, still optimized to no rows.
SELECT 'date32_eq_neq_dt64_midnight';
SELECT count() FROM 04499_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime64('2024-01-01 00:00:00.000', 3)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_date32
WHERE d = toDate32('2024-01-01') AND d != toDateTime64('2024-01-01 00:00:00.000', 3)
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE 04499_date;
DROP TABLE 04499_date32;

-- ---------------------------------------------------------------------------------------------------
-- Float64 -> Float32 narrowing
-- ---------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS 04499_float32;
CREATE TABLE 04499_float32 (f Float32) ENGINE = Memory;
INSERT INTO 04499_float32 VALUES (toFloat32(0.1)), (toFloat32(0.5));

-- equals + notEquals against a `Float64` constant that is NOT exactly representable in `Float32`:
-- `0.1` narrowed to `Float32` differs from the `Float64` `0.1`, and the column value promotes to
-- `Float64` for the comparison, so `f != toFloat64(0.1)` is true and the row must be kept. The chain
-- must not be folded to `false` by narrowing the `Float64` constant onto the `Float32` value.
SELECT 'float_eq_neq_narrowing';
SELECT count() FROM 04499_float32
WHERE f = toFloat32(0.1) AND f != toFloat64(0.1)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_float32
WHERE f = toFloat32(0.1) AND f != toFloat64(0.1)
SETTINGS optimize_redundant_comparisons = 1;

-- A `Float64` constant that IS exactly representable in `Float32` (`0.5`) round-trips exactly, so the
-- contradiction `f = 0.5 AND f != 0.5` is still optimized to no rows with or without the setting.
SELECT 'float_eq_neq_lossless';
SELECT count() FROM 04499_float32
WHERE f = toFloat32(0.5) AND f != toFloat64(0.5)
SETTINGS optimize_redundant_comparisons = 0;
SELECT count() FROM 04499_float32
WHERE f = toFloat32(0.5) AND f != toFloat64(0.5)
SETTINGS optimize_redundant_comparisons = 1;

DROP TABLE 04499_float32;
