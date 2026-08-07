-- Regression test for STID 0937-2022:
-- UndefinedBehaviorSanitizer: signed integer overflow in `base/base/Decimal.cpp`
-- inside `Decimal<long>::operator*=` / `operator+=` when `readDigits` accumulates
-- decimal digits past the underlying `Int64` range.
--
-- Stack trace from AST fuzzer (master + PR #100177 + PR #104545 over 4 days):
--   #0 DB::Decimal<long>::operator*=(long const&)   base/base/Decimal.cpp:24
--   #1 DB::readDigits<false, DB::Decimal<long>>     src/IO/readDecimalText.h:108
--   #2 DB::readDecimalText<DB::Decimal<long>, bool> src/IO/readDecimalText.h:155
--   #3 DB::tryReadDecimalText<DB::Decimal<long>>    src/IO/readDecimalText.h:199
--   #4 DB::normalizeParameter(...)                  src/AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesHelpers.cpp:58
--
-- `normalizeParameter` passes a string-typed timeseries parameter to
-- `tryReadDecimalText(buf, value, 20, scale)` with the underlying type
-- `Decimal<Int64>`. The digit accumulation `x *= 10; x += digit;` then overflowed
-- `Int64` once the partial value crossed `2^63`, which UBSAN flagged as undefined
-- behaviour. The fix marks `Decimal<T>`'s arithmetic operator overloads with
-- `NO_SANITIZE_UNDEFINED` (matching the sibling `addOverflow` / `negateOverflow`
-- helpers in the same file); ClickHouse already relies on well-defined
-- two's-complement wrap-around in these operators (digit accumulation, running
-- sums, scale conversion, etc.) and uses the explicit `common::*Overflow` helpers
-- in `base/arithmeticOverflow.h` wherever overflow detection is required.

SET allow_experimental_ts_to_grid_aggregate_function = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_decimal_overflow;
CREATE TABLE ts_decimal_overflow (timestamp DateTime64(3) NOT NULL, value Float64 NOT NULL)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ts_decimal_overflow VALUES ('2024-01-01 00:00:00', 1.0);

-- Case 1: 20-digit string parameter triggers `Decimal<long>::operator*=` overflow
-- inside `readDigits` (multiplying the accumulator by 10 once the partial value
-- crosses `2^63`). Before the fix UBSAN aborted the build; after the fix the
-- accumulation wraps and the subsequent scale-multiplier `common::mulOverflow`
-- check in `tryReadDecimalText` rejects the value with `BAD_ARGUMENTS`.
SELECT timeSeriesResampleToGridWithStaleness('99999999999999999999', '99999999999999999998', '1', '1')(timestamp, value)
FROM ts_decimal_overflow FORMAT Null;  -- { serverError BAD_ARGUMENTS }

-- Case 2: 19-digit string parameter just over `Int64` max. Triggers the
-- `operator+=` flavour (the AST fuzzer's `9223372036854775800 + 8` shape).
SELECT timeSeriesResampleToGridWithStaleness('9223372036854775808', '9223372036854775816', '1', '1')(timestamp, value)
FROM ts_decimal_overflow FORMAT Null;  -- { serverError BAD_ARGUMENTS }

-- Case 3: A string-typed parameter that parses cleanly within `Int64` must still
-- work (regression guard — make sure the `NO_SANITIZE_UNDEFINED` attribute does
-- not silently change the value for normal-range inputs).
SELECT length(timeSeriesResampleToGridWithStaleness('1577836800', '1577836810', '1', '1')(timestamp, value)) AS grid_len
FROM ts_decimal_overflow;

DROP TABLE ts_decimal_overflow;
