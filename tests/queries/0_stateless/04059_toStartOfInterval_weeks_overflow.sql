-- Regression test: overflow in toStartOfInterval when converting between interval kinds with origin argument
-- https://github.com/ClickHouse/ClickHouse/issues/100787

-- weeks * 7 overflow
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalWeek(9223372036854775807), toDate32('2023-10-01')); -- { serverError DECIMAL_OVERFLOW }

-- quarters * 3 overflow
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalQuarter(9223372036854775807), toDate32('2023-10-01')); -- { serverError DECIMAL_OVERFLOW }

-- years * 12 overflow
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalYear(9223372036854775807), toDate32('2023-10-01')); -- { serverError DECIMAL_OVERFLOW }
