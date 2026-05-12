-- Test: exercises `DateLUTImpl::toMillisecond` for negative `DateTime64` (pre-1970 dates with subseconds)
-- Covers: src/Common/DateLUTImpl.cpp:228 — `if (datetime.value < 0 && components.fractional)` branch
-- This branch normalizes the (whole, fractional) split for negative values.
-- Without it, `toMillisecond(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))` returns
-- a wrapped UInt16 (~65036) instead of 500.

-- Negative DateTime64 with whole=0 (right at the epoch boundary)
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.123', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.001', 3, 'UTC'));

-- Negative DateTime64 with whole != 0 (fractional sign flipped by splitWithScaleMultiplier)
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:55.123', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-01-01 00:00:00.456', 3, 'UTC'));

-- Different scales (exercises scale_multiplier > and < microsecond_multiplier paths)
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.12', 2, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.123456', 6, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.123456789', 9, 'UTC'));

-- Materialized (non-constant) variants to ensure column path is taken
SELECT toMillisecond(materialize(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')));
SELECT toMillisecond(materialize(toDateTime64('1969-12-31 23:59:59.123456', 6, 'UTC')));
