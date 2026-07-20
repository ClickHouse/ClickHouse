-- Test: exercises `toMillisecond` with negative DateTime64 values (pre-epoch),
-- which triggers the `if (datetime.value < 0 && components.fractional)` branch
-- in `DateLUTImpl::toMillisecond` at src/Common/DateLUTImpl.cpp:228.
-- The PR's own test (02998_to_milliseconds.sql) only covers positive timestamps
-- (2023-04-21), so the entire negative branch (and both sub-paths
-- `whole == 0` vs `whole != 0` of the ternary at line 230) is uncovered.

-- Sub-branch 1: -1 < value < 0 (whole == 0, ternary takes +1 path)
-- Value = -1 sec + 0.123 sec = -877 (scale 3); whole=0, fractional=-877
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.123', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'));

-- Sub-branch 2: value < -1 sec (whole != 0, ternary takes -1 path)
SELECT toMillisecond(toDateTime64('1969-12-30 12:34:56.789', 3, 'UTC'));
SELECT toMillisecond(toDateTime64('1900-01-01 00:00:00.500', 3, 'UTC'));

-- Different scale multipliers exercise the rescale branches (lines 234-237)
-- with negative values
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.456789', 6, 'UTC'));
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59.123456789', 9, 'UTC'));

-- Negative value with zero fractional: branch NOT taken (fractional == 0)
SELECT toMillisecond(toDateTime64('1969-12-31 23:59:59', 0, 'UTC'));

-- Non-constant column path (materialize forces vector execution)
SELECT toMillisecond(materialize(toDateTime64('1969-12-31 23:59:59.123', 3, 'UTC')));
SELECT toMillisecond(materialize(toDateTime64('1969-12-30 12:34:56.789', 3, 'UTC')));
