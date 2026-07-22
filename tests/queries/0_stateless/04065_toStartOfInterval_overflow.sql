-- Test that toStartOfInterval with extreme DateTime64 values returns well-defined results without overflow.
-- Use reinterpret to inject raw internal values because CAST clamps to the valid DateTime64 range.
-- The start of the interval is computed by floor division, so no intermediate value can exceed the input.

-- Millisecond interval with scale=6 (microseconds)
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \'UTC\')'), toIntervalMillisecond(1));

-- Millisecond interval with scale=9 (nanoseconds)
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(9, \'UTC\')'), toIntervalMillisecond(1));

-- Microsecond interval with scale=9 (nanoseconds)
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(9, \'UTC\')'), toIntervalMicrosecond(1));

-- Three-argument overload: a negative origin makes the difference to an extreme time exceed Int64, but the
-- interval start still fits into the result type, so the result must be returned, not an overflow error
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \'UTC\')'), toIntervalMillisecond(1), reinterpret(toInt64(-2), 'DateTime64(6, \'UTC\')'));

-- Three-argument overload with an extreme time and the epoch origin has a well-defined result
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \'UTC\')'), toIntervalMillisecond(1), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));

-- Large intervals go through calendar arithmetic which requires the difference to fit into Int64
SELECT toStartOfInterval(reinterpret(toInt64(9223372036854775806), 'DateTime64(6, \'UTC\')'), toIntervalSecond(1), reinterpret(toInt64(-2), 'DateTime64(6, \'UTC\')')); -- { serverError DECIMAL_OVERFLOW }
