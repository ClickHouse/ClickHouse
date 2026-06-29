-- Test that toStartOfInterval with extreme DateTime64 values throws overflow instead of UB.
-- Use reinterpret to inject raw internal values because CAST clamps to the valid DateTime64 range.

-- Millisecond interval with scale=6 (microseconds): scale_diff=1000, t + 500 overflows
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(6)'), toIntervalMillisecond(1)); -- { serverError DECIMAL_OVERFLOW }

-- Millisecond interval with scale=9 (nanoseconds): scale_diff=1000000, t + 500000 overflows
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(9)'), toIntervalMillisecond(1)); -- { serverError DECIMAL_OVERFLOW }

-- Microsecond interval with scale=9 (nanoseconds): scale_diff=1000, t + 500 overflows
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(9)'), toIntervalMicrosecond(1)); -- { serverError DECIMAL_OVERFLOW }
