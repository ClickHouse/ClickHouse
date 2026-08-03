-- Rounding a subsecond value near the lower bound of Int64 down to the start of an interval used to
-- multiply the rounded quotient back without an overflow check, silently wrapping to a positive value
-- (undefined behavior reported by the UBSan build). Now it throws `DECIMAL_OVERFLOW`.
-- See https://github.com/ClickHouse/ClickHouse/issues/101096

-- Equal scales (DateTime64(9) with a nanosecond interval).
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(9)'), toIntervalNanosecond(17)); -- { serverError DECIMAL_OVERFLOW }

-- The value scale is smaller than the interval scale (DateTime64(8) with a nanosecond interval).
SELECT toStartOfInterval(reinterpret(toInt64(-922337203685477580), 'DateTime64(8)'), toIntervalNanosecond(17)); -- { serverError DECIMAL_OVERFLOW }

-- The same for the microsecond and millisecond specializations.
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(6)'), toIntervalMicrosecond(17)); -- { serverError DECIMAL_OVERFLOW }
SELECT toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(3)'), toIntervalMillisecond(17)); -- { serverError DECIMAL_OVERFLOW }

-- Negative values that do not overflow must still round downwards (towards negative infinity).
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-100), 'DateTime64(9)'), toIntervalNanosecond(17)), 'Int64');
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-100), 'DateTime64(8)'), toIntervalNanosecond(1700)), 'Int64');

-- The three-argument overload with an `origin` near the lower bound of Int64. The caller requires
-- `origin <= value`, so the subtraction of the remainder can never go below the origin, but the
-- helper checks it anyway so that it is safe on its own. These must round downwards, not overflow.
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(9)'), toIntervalNanosecond(5), reinterpret(toInt64(-9223372036854775807), 'DateTime64(9)')), 'Int64');
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-922337203685477575), 'DateTime64(8)'), toIntervalNanosecond(7), reinterpret(toInt64(-922337203685477580), 'DateTime64(8)')), 'Int64');
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(9)'), toIntervalMicrosecond(2), reinterpret(toInt64(-9223372036854775808), 'DateTime64(9)')), 'Int64');

-- Scaling the origin up to the interval scale can still overflow, and must throw.
SELECT toStartOfInterval(reinterpret(toInt64(-922337203685477580), 'DateTime64(8)'), toIntervalNanosecond(7), reinterpret(toInt64(-922337203685477581), 'DateTime64(8)')); -- { serverError DECIMAL_OVERFLOW }
