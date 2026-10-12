-- Tags: no-fasttest
-- no-fasttest: JIT compilation is not available in fasttest

-- Comparisons on date and time types give the same results compiled and interpreted.
-- Each pair packs its six comparisons into one expression, so they are fused with `plus` and `multiply` and compiled.

DROP TABLE IF EXISTS jit_dates;
DROP TABLE IF EXISTS jit_dates_result_compiled;
DROP TABLE IF EXISTS jit_dates_result_interpreted;

CREATE TABLE jit_dates
(
    id UInt32,
    d Date,
    d2 Date,
    d32 Date32,
    d32b Date32,
    dt DateTime('UTC'),
    dt2 DateTime('Asia/Tokyo'),
    dt64 DateTime64(3, 'UTC'),
    dt64b DateTime64(3, 'Asia/Tokyo'),
    dt64_9 DateTime64(9, 'UTC'),
    nd Nullable(Date),
    ndt Nullable(DateTime('UTC')),
    i Int64,
    u UInt32,
    f Float64
) ENGINE = MergeTree ORDER BY id;

-- Edge values: the epoch, the ends of the ranges, values before the epoch, equal instants in different time zones,
-- equal days as `Date` and `Date32`, and the largest `DateTime` against a `DateTime64(9)`.
INSERT INTO jit_dates SELECT
    1, '1970-01-01', '1970-01-01', '1900-01-01', '1970-01-01', toDateTime(0, 'UTC'), toDateTime(0, 'UTC'),
    fromUnixTimestamp64Milli(0, 'UTC'), fromUnixTimestamp64Milli(-1, 'UTC'), fromUnixTimestamp64Nano(0, 'UTC'),
    NULL, NULL, -1, 0, -0.5;
INSERT INTO jit_dates SELECT
    2, '2149-06-06', '2149-06-05', '2299-12-31', '2299-12-31', toDateTime(4294967295, 'UTC'), toDateTime(4294967294, 'UTC'),
    fromUnixTimestamp64Milli(4294967295000, 'UTC'), fromUnixTimestamp64Milli(4294967295000, 'UTC'), fromUnixTimestamp64Nano(4294967295000000000, 'UTC'),
    '2149-06-06', toDateTime(4294967295, 'UTC'), 4294967296, 4294967295, 4294967295.5;
INSERT INTO jit_dates SELECT
    3, '2026-10-12', '2026-10-13', '2026-10-12', '1969-12-31', toDateTime(1791720000, 'UTC'), toDateTime(1791720000, 'UTC'),
    fromUnixTimestamp64Milli(1791720000000, 'UTC'), fromUnixTimestamp64Milli(1791720000001, 'UTC'), fromUnixTimestamp64Nano(1791720000000000001, 'UTC'),
    '2026-10-12', toDateTime(1791720000, 'UTC'), 1791720000, 1791720001, 1791720000;

-- Many more rows, with equal and different values on both sides of each pair.
INSERT INTO jit_dates
SELECT
    100 + number,
    toDate(number * 37 % 65536),
    toDate(number * 41 % 65536),
    toDate32(-25567 + number * 53 % 146097),
    toDate32(-25567 + number * 37 % 146097),
    toDateTime(number * 2654435761 % 4294967296, 'UTC'),
    toDateTime(number * 2654435789 % 4294967296, 'Asia/Tokyo'),
    fromUnixTimestamp64Milli(toInt64(number * 2654435761 % 4294967296) * 1000 + number % 3 - 1, 'UTC'),
    fromUnixTimestamp64Milli(toInt64(number * 2654435761 % 4294967296) * 1000 + number % 5 - 2, 'Asia/Tokyo'),
    fromUnixTimestamp64Nano(toInt64(number * 2654435761 % 4294967296) * 1000000000 + number % 3 - 1, 'UTC'),
    if(number % 7 = 0, NULL, toDate(number * 37 % 65536)),
    if(number % 11 = 0, NULL, toDateTime(number * 2654435789 % 4294967296, 'UTC')),
    toInt64(number * 2654435761 % 4294967296) - number % 3 + 1,
    toUInt32(number * 2654435789 % 4294967296),
    toFloat64(number * 2654435761 % 4294967296) + 0.5 * (number % 3) - 0.5
FROM numbers(3000);

CREATE TABLE jit_dates_result_compiled
(
    id UInt32,
    date_date UInt8, date32_date32 UInt8, datetime_datetime UInt8, date_date32 UInt8,
    datetime64_datetime64 UInt8, datetime_datetime64 UInt8, datetime64_datetime UInt8, datetime_datetime64_9 UInt8,
    datetime_int UInt8, datetime_uint UInt8, datetime_float UInt8,
    nullable_date Nullable(UInt8), nullable_datetime Nullable(UInt8),
    datetime_const UInt8, date_const UInt8, datetime64_const UInt8
) ENGINE = MergeTree ORDER BY id;

CREATE TABLE jit_dates_result_interpreted AS jit_dates_result_compiled;

SET log_queries = 1;

-- All pairs that can be compiled. Each column is a bitmask of the six comparisons.
INSERT INTO jit_dates_result_compiled
SELECT
    id,
    (d = d2) + 2 * (d != d2) + 4 * (d < d2) + 8 * (d > d2) + 16 * (d <= d2) + 32 * (d >= d2),
    (d32 = d32b) + 2 * (d32 != d32b) + 4 * (d32 < d32b) + 8 * (d32 > d32b) + 16 * (d32 <= d32b) + 32 * (d32 >= d32b),
    (dt = dt2) + 2 * (dt != dt2) + 4 * (dt < dt2) + 8 * (dt > dt2) + 16 * (dt <= dt2) + 32 * (dt >= dt2),
    (d = d32) + 2 * (d != d32) + 4 * (d < d32) + 8 * (d > d32) + 16 * (d <= d32) + 32 * (d >= d32),
    (dt64 = dt64b) + 2 * (dt64 != dt64b) + 4 * (dt64 < dt64b) + 8 * (dt64 > dt64b) + 16 * (dt64 <= dt64b) + 32 * (dt64 >= dt64b),
    (dt = dt64) + 2 * (dt != dt64) + 4 * (dt < dt64) + 8 * (dt > dt64) + 16 * (dt <= dt64) + 32 * (dt >= dt64),
    (dt64 = dt2) + 2 * (dt64 != dt2) + 4 * (dt64 < dt2) + 8 * (dt64 > dt2) + 16 * (dt64 <= dt2) + 32 * (dt64 >= dt2),
    (dt = dt64_9) + 2 * (dt != dt64_9) + 4 * (dt < dt64_9) + 8 * (dt > dt64_9) + 16 * (dt <= dt64_9) + 32 * (dt >= dt64_9),
    (dt = i) + 2 * (dt != i) + 4 * (dt < i) + 8 * (dt > i) + 16 * (dt <= i) + 32 * (dt >= i),
    (dt = u) + 2 * (dt != u) + 4 * (dt < u) + 8 * (dt > u) + 16 * (dt <= u) + 32 * (dt >= u),
    (dt = f) + 2 * (dt != f) + 4 * (dt < f) + 8 * (dt > f) + 16 * (dt <= f) + 32 * (dt >= f),
    (nd = d) + 2 * (nd != d) + 4 * (nd < d) + 8 * (nd > d) + 16 * (nd <= d) + 32 * (nd >= d),
    (ndt = dt) + 2 * (ndt != dt) + 4 * (ndt < dt) + 8 * (ndt > dt) + 16 * (ndt <= dt) + 32 * (ndt >= dt),
    (dt = toDateTime(1791720000, 'UTC')) + 2 * (dt != toDateTime(1791720000, 'UTC')) + 4 * (dt < toDateTime(1791720000, 'UTC'))
        + 8 * (dt > toDateTime(1791720000, 'UTC')) + 16 * (dt <= toDateTime(1791720000, 'UTC')) + 32 * (dt >= toDateTime(1791720000, 'UTC')),
    (d = toDate(20738)) + 2 * (d != toDate(20738)) + 4 * (d < toDate(20738)) + 8 * (d > toDate(20738)) + 16 * (d <= toDate(20738)) + 32 * (d >= toDate(20738)),
    (dt64 = toDateTime(1791720000, 'UTC')) + 2 * (dt64 != toDateTime(1791720000, 'UTC')) + 4 * (dt64 < toDateTime(1791720000, 'UTC'))
        + 8 * (dt64 > toDateTime(1791720000, 'UTC')) + 16 * (dt64 <= toDateTime(1791720000, 'UTC')) + 32 * (dt64 >= toDateTime(1791720000, 'UTC'))
FROM jit_dates
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;

INSERT INTO jit_dates_result_interpreted
SELECT
    id,
    (d = d2) + 2 * (d != d2) + 4 * (d < d2) + 8 * (d > d2) + 16 * (d <= d2) + 32 * (d >= d2),
    (d32 = d32b) + 2 * (d32 != d32b) + 4 * (d32 < d32b) + 8 * (d32 > d32b) + 16 * (d32 <= d32b) + 32 * (d32 >= d32b),
    (dt = dt2) + 2 * (dt != dt2) + 4 * (dt < dt2) + 8 * (dt > dt2) + 16 * (dt <= dt2) + 32 * (dt >= dt2),
    (d = d32) + 2 * (d != d32) + 4 * (d < d32) + 8 * (d > d32) + 16 * (d <= d32) + 32 * (d >= d32),
    (dt64 = dt64b) + 2 * (dt64 != dt64b) + 4 * (dt64 < dt64b) + 8 * (dt64 > dt64b) + 16 * (dt64 <= dt64b) + 32 * (dt64 >= dt64b),
    (dt = dt64) + 2 * (dt != dt64) + 4 * (dt < dt64) + 8 * (dt > dt64) + 16 * (dt <= dt64) + 32 * (dt >= dt64),
    (dt64 = dt2) + 2 * (dt64 != dt2) + 4 * (dt64 < dt2) + 8 * (dt64 > dt2) + 16 * (dt64 <= dt2) + 32 * (dt64 >= dt2),
    (dt = dt64_9) + 2 * (dt != dt64_9) + 4 * (dt < dt64_9) + 8 * (dt > dt64_9) + 16 * (dt <= dt64_9) + 32 * (dt >= dt64_9),
    (dt = i) + 2 * (dt != i) + 4 * (dt < i) + 8 * (dt > i) + 16 * (dt <= i) + 32 * (dt >= i),
    (dt = u) + 2 * (dt != u) + 4 * (dt < u) + 8 * (dt > u) + 16 * (dt <= u) + 32 * (dt >= u),
    (dt = f) + 2 * (dt != f) + 4 * (dt < f) + 8 * (dt > f) + 16 * (dt <= f) + 32 * (dt >= f),
    (nd = d) + 2 * (nd != d) + 4 * (nd < d) + 8 * (nd > d) + 16 * (nd <= d) + 32 * (nd >= d),
    (ndt = dt) + 2 * (ndt != dt) + 4 * (ndt < dt) + 8 * (ndt > dt) + 16 * (ndt <= dt) + 32 * (ndt >= dt),
    (dt = toDateTime(1791720000, 'UTC')) + 2 * (dt != toDateTime(1791720000, 'UTC')) + 4 * (dt < toDateTime(1791720000, 'UTC'))
        + 8 * (dt > toDateTime(1791720000, 'UTC')) + 16 * (dt <= toDateTime(1791720000, 'UTC')) + 32 * (dt >= toDateTime(1791720000, 'UTC')),
    (d = toDate(20738)) + 2 * (d != toDate(20738)) + 4 * (d < toDate(20738)) + 8 * (d > toDate(20738)) + 16 * (d <= toDate(20738)) + 32 * (d >= toDate(20738)),
    (dt64 = toDateTime(1791720000, 'UTC')) + 2 * (dt64 != toDateTime(1791720000, 'UTC')) + 4 * (dt64 < toDateTime(1791720000, 'UTC'))
        + 8 * (dt64 > toDateTime(1791720000, 'UTC')) + 16 * (dt64 <= toDateTime(1791720000, 'UTC')) + 32 * (dt64 >= toDateTime(1791720000, 'UTC'))
FROM jit_dates
SETTINGS compile_expressions = 0;

SELECT 'rows differing, compiled vs interpreted';
SELECT count() FROM (SELECT * FROM jit_dates_result_compiled EXCEPT SELECT * FROM jit_dates_result_interpreted);
SELECT count() FROM (SELECT * FROM jit_dates_result_interpreted EXCEPT SELECT * FROM jit_dates_result_compiled);
SELECT 'rows', count() FROM jit_dates_result_compiled;

SELECT 'edge rows';
SELECT * FROM jit_dates_result_compiled WHERE id < 100 ORDER BY id;

-- The same comparisons as a `SELECT`, to check with the profile events that every function in it ran compiled:
-- the three columns have 7 functions each (3 comparisons, 2 `multiply`, 2 `plus`).
SELECT
    (d = d2) + 2 * (d != d2) + 4 * (d < d2),
    (d = d32) + 2 * (d32 < d) + 4 * (d32 >= d),
    (dt = dt2) + 2 * (dt64 > dt) + 4 * (dt64 <= dt64b)
FROM jit_dates FORMAT Null
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_date_comparisons_compiled';

-- Pairs that stay interpreted: `Date` vs `DateTime` needs the time zone, `DateTime64` values of different scales are rescaled
-- with an overflow check, and a `Date32` vs a `DateTime64` needs both.
SELECT
    (d = dt) + 2 * (d < dt),
    (dt64 = dt64_9) + 2 * (dt64 < dt64_9),
    (d32 = dt64) + 2 * (d32 < dt64)
FROM jit_dates FORMAT Null
SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0, log_comment = 'jit_date_comparisons_not_compiled';

SYSTEM FLUSH LOGS query_log;

-- All 21 functions of the query ran compiled, on every row. `InterpretedFunctionRows` is not checked: reading may add
-- interpreted functions of its own (for example `merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability`).
SELECT 'compiled', ProfileEvents['CompiledFunctionRows'] = 21 * (SELECT count() FROM jit_dates)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'jit_date_comparisons_compiled' AND event_date >= yesterday();

SELECT 'not compiled', ProfileEvents['InterpretedFunctionRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'jit_date_comparisons_not_compiled' AND event_date >= yesterday();

DROP TABLE jit_dates;
DROP TABLE jit_dates_result_compiled;
DROP TABLE jit_dates_result_interpreted;
