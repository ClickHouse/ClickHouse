-- Companion to 04401_date_time_overflow_behavior_numeric_casts.sql, split off to keep each test
-- under the flaky check runtime cap. This half covers the value-materialization path:
-- convertFieldToType, reached by INSERT ... VALUES expressions and the values() table function.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101131

SET session_timezone = 'UTC';
SET allow_experimental_time_time64_type = 1;

-- INSERT ... VALUES with a numeric expression is coerced through convertFieldToType, which
-- previously stored the raw out-of-range value regardless of the setting. It now applies the
-- same rule as toTime / toDateTime / CAST. (Plain integer literals in VALUES go through the
-- text serializer instead, which is a separate pre-existing path shared by all input formats.)
-- convertFieldToType is reached only when expression templates are disabled: under the default
-- input_format_values_deduce_templates_of_expressions = 1 the row is parsed by an expression
-- template that ends in castColumn, i.e. the CAST implementation, so every assertion below would
-- compare CAST against itself. That is why the VALUES sections pin the setting off (the full
-- mechanism is spelled out above the fractional Date32 block) and restore the default after.
DROP TABLE IF EXISTS t_vals_time;
DROP TABLE IF EXISTS t_vals_datetime;

SELECT '-- throw: out-of-range numeric VALUES expression must raise';
-- The overflow of a VALUES expression may surface as a client or a server error depending
-- on async_insert (the client parses VALUES data), so the error hint below accepts either.
SET date_time_overflow_behavior = 'throw';
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_vals_time (x Time) ENGINE = Memory;
CREATE TABLE t_vals_datetime (x DateTime) ENGINE = Memory;
INSERT INTO t_vals_time VALUES (9999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_time VALUES (-9999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_datetime VALUES (99999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_datetime VALUES (-1 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- In-range control in the same mode: the raise above must be selective, not a blanket rejection.
INSERT INTO t_vals_time VALUES (3599999 + 0), (-3599999 + 0);
INSERT INTO t_vals_datetime VALUES (4294967295 + 0), (0 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;
SELECT toInt64(x) FROM t_vals_datetime ORDER BY x;
TRUNCATE TABLE t_vals_time;
TRUNCATE TABLE t_vals_datetime;

SELECT '-- saturate: out-of-range numeric VALUES expression must clamp to the boundary';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_vals_time VALUES (9999999 + 0), (-9999999 + 0);
INSERT INTO t_vals_datetime VALUES (99999999999 + 0), (-1 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;
SELECT toInt64(x) FROM t_vals_datetime ORDER BY x;

SELECT '-- ignore: out-of-range numeric VALUES expression must clamp too, not store the raw value';
TRUNCATE TABLE t_vals_time;
TRUNCATE TABLE t_vals_datetime;
SET date_time_overflow_behavior = 'ignore';
INSERT INTO t_vals_time VALUES (9999999 + 0), (-9999999 + 0);
INSERT INTO t_vals_datetime VALUES (99999999999 + 0), (-1 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;
SELECT toInt64(x) FROM t_vals_datetime ORDER BY x;

SELECT '-- in-range numeric VALUES expression is unchanged in all modes';
TRUNCATE TABLE t_vals_time;
INSERT INTO t_vals_time VALUES (3600 + 0), (-3600 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;

DROP TABLE t_vals_time;
DROP TABLE t_vals_datetime;

-- Date / Date32 VALUES expressions also reach convertFieldToType (still with templates disabled, see
-- above) and used to ignore the setting: Date/Date32 returned Null (defaulting) or narrowed the raw
-- value through the serializer. They must respect date_time_overflow_behavior with the same
-- day-number / unix-timestamp interpretation as CAST.
SELECT '-- throw: out-of-range Date / Date32 VALUES expression must raise';
SET date_time_overflow_behavior = 'throw';
CREATE TABLE t_vals_date (x Date) ENGINE = Memory;
CREATE TABLE t_vals_date32 (x Date32) ENGINE = Memory;
INSERT INTO t_vals_date VALUES (99999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date VALUES (-1 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date32 VALUES (999999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date32 VALUES (-999999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- saturate: out-of-range Date / Date32 VALUES expression must clamp to the boundary';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_vals_date VALUES (99999999999 + 0);
INSERT INTO t_vals_date32 VALUES (999999999999 + 0), (-999999999999 + 0);
SELECT toString(x) FROM t_vals_date ORDER BY x;
SELECT toString(x) FROM t_vals_date32 ORDER BY x;

SELECT '-- ignore: out-of-range Date / Date32 VALUES expression must clamp too';
TRUNCATE TABLE t_vals_date;
TRUNCATE TABLE t_vals_date32;
SET date_time_overflow_behavior = 'ignore';
INSERT INTO t_vals_date VALUES (99999999999 + 0);
INSERT INTO t_vals_date32 VALUES (999999999999 + 0), (-999999999999 + 0);
SELECT toString(x) FROM t_vals_date ORDER BY x;
SELECT toString(x) FROM t_vals_date32 ORDER BY x;

DROP TABLE t_vals_date;
DROP TABLE t_vals_date32;
SET input_format_values_deduce_templates_of_expressions = 1;

-- Non-Int64/UInt64 field types that evaluateConstantExpression can produce (Float64, wide integers)
-- must be coerced through the same overflow-aware path for every target, not fall through to
-- TYPE_MISMATCH or the raw serializer.
SELECT '-- saturate: Float64 and wide-integer VALUES expressions clamp for every target';
SET date_time_overflow_behavior = 'saturate';
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_wide (d Date, d32 Date32, dt DateTime, t Time) ENGINE = Memory;
INSERT INTO t_wide VALUES (1e12 + 0.0, 1e12 + 0.0, 1e12 + 0.0, 1e12 + 0.0);
INSERT INTO t_wide VALUES (toUInt128(99999999999) + toUInt128(0), toInt128(999999999999) + toInt128(0), toUInt256(99999999999) + toUInt256(0), toInt256(999999999999) + toInt256(0));
SELECT toString(d), toString(d32), toString(dt), toString(t) FROM t_wide ORDER BY dt;
DROP TABLE t_wide;
SET input_format_values_deduce_templates_of_expressions = 1;

-- The VALUES/INSERT coercion path must agree with the numeric CAST path for the same source value,
-- especially across the Date day-number / unix-timestamp boundary (65535 is the last day number,
-- 65536 is reinterpreted as a unix timestamp). The two SELECTs below must print identical values.
SELECT '-- VALUES coercion agrees with CAST across the Date day-number / timestamp boundary (saturate)';
SET date_time_overflow_behavior = 'saturate';
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_parity (x Date) ENGINE = Memory;
INSERT INTO t_parity VALUES (65535 + 0), (65536 + 0), (100000 + 0), (5662310399 + 0);
SELECT 'VALUES', toString(x) FROM t_parity ORDER BY x;
SELECT 'CAST  ', toString(CAST(v AS Date)) FROM (SELECT arrayJoin([65535, 65536, 100000, 5662310399]) AS v) ORDER BY CAST(v AS Date);
DROP TABLE t_parity;
SET input_format_values_deduce_templates_of_expressions = 1;

-- Same parity requirement at the Date32 boundary, for a FRACTIONAL day number. This pins the coerce
-- helper's day-number/timestamp predicate to the transform's own form: a fractional value just below
-- DATE_LUT_MAX_EXTEND_DAY_NUM stays a day number and keeps ~9999, while one just above is
-- reinterpreted as seconds and lands in 1970. 2932896 is the boundary, so 2932895.5 and 2932896.5
-- straddle it and land on different arms; 2932894.5 is a control inside the day-number domain. The
-- pair cannot pass by routing everything to one arm. Both sides are printed per row, so a future
-- divergence diffs.
--
-- Reaching the coerce helper needs BOTH: an expression rather than a bare literal (`+ 0`; a literal
-- goes through the text serializer, see the note above the t_vals_time section), AND templates
-- disabled. With the default input_format_values_deduce_templates_of_expressions = 1 the row is
-- parsed by an expression template whose evaluateAll ends in castColumn, i.e. the CAST
-- implementation itself, so both sides of this assertion would agree no matter what the helper does.
SELECT '-- VALUES coercion agrees with CAST for a fractional Date32 day number at the boundary (ignore)';
SET date_time_overflow_behavior = 'ignore';
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_parity_frac (v Float64, x Date32) ENGINE = Memory;
INSERT INTO t_parity_frac VALUES (2932894.5, 2932894.5 + 0), (2932895.5, 2932895.5 + 0), (2932896.5, 2932896.5 + 0);
SELECT toString(v), 'VALUES', toString(x), 'CAST', toString(CAST(v AS Date32)) FROM t_parity_frac ORDER BY v;
DROP TABLE t_parity_frac;

SELECT '-- VALUES coercion agrees with CAST for a fractional Date32 day number at the boundary (saturate)';
SET date_time_overflow_behavior = 'saturate';
CREATE TABLE t_parity_frac (v Float64, x Date32) ENGINE = Memory;
INSERT INTO t_parity_frac VALUES (2932894.5, 2932894.5 + 0), (2932895.5, 2932895.5 + 0), (2932896.5, 2932896.5 + 0);
SELECT toString(v), 'VALUES', toString(x), 'CAST', toString(CAST(v AS Date32)) FROM t_parity_frac ORDER BY v;
DROP TABLE t_parity_frac;
SET input_format_values_deduce_templates_of_expressions = 1;

-- The boundary day number ITSELF (an integer, not a fraction) is the carrier for the strict-vs-
-- non-strict form of the same predicate: `>` keeps 2932896 a day number (9999-12-31) while `>=`
-- reinterprets it as seconds and lands in 1970. 2932895 and 2932897 are controls on either side.
SELECT '-- VALUES coercion agrees with CAST for the boundary Date32 day number itself (ignore)';
SET date_time_overflow_behavior = 'ignore';
SET input_format_values_deduce_templates_of_expressions = 0;
CREATE TABLE t_parity_bnd (v Int64, x Date32) ENGINE = Memory;
INSERT INTO t_parity_bnd VALUES (2932895, 2932895 + 0), (2932896, 2932896 + 0), (2932897, 2932897 + 0);
SELECT toString(v), 'VALUES', toString(x), 'CAST', toString(CAST(v AS Date32)) FROM t_parity_bnd ORDER BY v;
DROP TABLE t_parity_bnd;
SET input_format_values_deduce_templates_of_expressions = 1;

-- The `values` table function reaches the same helper through convertFieldToTypeOrThrow, with its own
-- default format settings (so it is not affected by the SET above), and needs no template opt-out.
SELECT '-- values() table function agrees with CAST for the same fractional Date32 day numbers';
SELECT toString(v), 'VALUES', toString(CAST(x AS Date32)), 'CAST', toString(CAST(v AS Date32))
FROM values('v Float64, x Date32', (2932894.5, 2932894.5), (2932895.5, 2932895.5), (2932896.5, 2932896.5)) ORDER BY v;
