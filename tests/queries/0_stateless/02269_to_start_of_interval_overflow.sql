select toStartOfInterval(toDateTime64('\0930-12-12 12:12:12.1234567', 3), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}

SELECT
    toDateTime64(-9223372036854775808, 1048575, toIntervalNanosecond(9223372036854775806), NULL),
    toStartOfInterval(toDateTime64(toIntervalNanosecond(toIntervalNanosecond(257), toDateTime64(toStartOfInterval(toDateTime64(NULL)))), '', 100), toIntervalNanosecond(toStartOfInterval(toDateTime64(toIntervalNanosecond(NULL), NULL)), -1)),
    toStartOfInterval(toDateTime64('\0930-12-12 12:12:12.1234567', 3), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}

-- Overflow in Week/Quarter/Year interval conversions (weeks*7, quarters*3, years*12) when using an origin.
-- These are UBSan-visible signed integer overflows (STID 1989-4fd7).
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalWeek(9223372036854775807), toDate32('2023-10-01')); -- {serverError DECIMAL_OVERFLOW}
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalQuarter(9223372036854775807), toDate32('2023-10-01')); -- {serverError DECIMAL_OVERFLOW}
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalYear(9223372036854775807), toDate32('2023-10-01')); -- {serverError DECIMAL_OVERFLOW}
