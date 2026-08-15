-- Regression test for a signed integer overflow in `DateLUTImpl::toStartOfMonthInterval` with an extreme
-- interval count. This is the `MONTH` sibling (plus `WEEK` and `DAY`) of the `toStartOfHourInterval` /
-- `toStartOfMinuteInterval` overflows covered by 04498 and 04499. `toStartOfInterval` only validates that
-- the interval value is positive, so a huge but positive `toIntervalMonth` reaches the out-of-LUT-range
-- branch of `toStartOfMonthInterval`, where the hand-rolled floored-division idiom `-((-rel + div - 1) / div)`
-- overflows on `-rel + div` (`22800 + 9223372036854775807 cannot be represented in type 'Int64'`). The
-- rounding now goes through the overflow-safe `roundDownToMultiple` helper; the `WEEK` and `DAY` out-of-range
-- paths shared the same `x + 1 - divisor` idiom and are fixed the same way. `QUARTER` delegates to the month
-- path after `quarters * 3`, which wrapped in UInt64 (a huge quarter count became a tiny month count that
-- rounded to the wrong boundary); the product is now saturated before delegating. Finally, the `WEEK` path
-- reconstructs a day number as `4 + rounded`; after `weeks * 7` is saturated, that boundary is far below any
-- representable day, so both the `DayNum` and `ExtendedDayNum` branches used to round FORWARD (through a
-- narrow cast) instead of flooring. The reconstructed day is now clamped into the representable range so an
-- extreme week count floors to the earliest representable boundary. Finally, the in-LUT `DAY` path for a
-- pre-epoch `ExtendedDayNum` used truncating division (`d / idays * idays`), which rounds a negative day
-- number toward zero (forward in time) instead of flooring; e.g. `toDate32('1969-12-31')` with a 10-day
-- interval returned `1970-01-01` instead of the `1969-12-22` bucket. That path now goes through the same
-- floored `roundDownToMultiple` helper.

-- Normal values must still be rounded down to the start of the interval correctly.
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 5 MONTH);
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 3 MONTH);
SELECT toStartOfInterval(toDate32('2021-06-22'), INTERVAL 5 MONTH);
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 2 WEEK);
SELECT toStartOfInterval(toDate32('2021-06-22'), INTERVAL 2 WEEK);
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 10 DAY);
SELECT toStartOfInterval(toDate32('2021-06-22'), INTERVAL 10 DAY);
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 2 QUARTER);
SELECT toStartOfInterval(toDate32('2021-06-22'), INTERVAL 2 QUARTER);

-- An extreme but positive `QUARTER` count must not wrap in `quarters * 3` down to a small month count and
-- silently round to that wrong boundary. `6148914691236517206 * 3` wraps to `2` in UInt64, so before the fix
-- this equalled `INTERVAL 2 MONTH`; with the saturated product it does not.
SELECT toStartOfInterval(toDate('2021-06-22'), INTERVAL 6148914691236517206 QUARTER)
     = toStartOfInterval(toDate('2021-06-22'), INTERVAL 2 MONTH);

-- An extreme but positive `WEEK` count near the epoch must floor to the earliest representable boundary, not
-- round FORWARD. `DayNum` branch: before the fix `toDate('1970-01-01')` returned `1970-01-04`; it must not
-- round past the input.
SELECT toStartOfInterval(toDate('1970-01-01'), INTERVAL 1317624576693539402 WEEK) <= toDate('1970-01-01');
-- `ExtendedDayNum` branch (Date32 input): before the fix `toDate32('1969-12-31')` rounded forward to
-- `1970-01-06`; with the reconstructed day clamped into range it floors to the earliest representable boundary.
SELECT toStartOfInterval(toDate32('1969-12-31'), INTERVAL 1317624576693539402 WEEK) = toDate('1973-10-02');
-- The default overload above narrows the `ExtendedDayNum` result through `Date`, which masks the actual
-- boundary. With `enable_extended_results_for_datetime_functions = 1` the `Date32` boundary is observable
-- directly: week boundaries live on the `4 + 7 * n` (Monday) lattice, so the earliest representable one is
-- `0000-01-03` (`0000-01-01` is a Saturday). Before the fix the clamp snapped to `0000-01-01`, off the
-- lattice; it must floor to the first representable Monday and land exactly on a week boundary.
SELECT toInt32(toStartOfInterval(toDate32('1969-12-31'), INTERVAL 1317624576693539402 WEEK)) = -719526
    SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toDayOfWeek(toStartOfInterval(toDate32('1969-12-31'), INTERVAL 1317624576693539402 WEEK)) = 1
    SETTINGS enable_extended_results_for_datetime_functions = 1;
-- Symmetric high bound: a positive out-of-range `ExtendedDayNum` (a `Date32` pushed past `9999-12-31` by
-- arithmetic) must clamp to the last representable Monday `9999-12-27` (day `2932892`), not to the raw
-- maximum day `9999-12-31` (a Friday, off the lattice). Before the fix the high clamp used `max_daynum`
-- directly and returned `9999-12-31`, breaking the `4 + 7 * n` invariant.
SELECT toInt32(toStartOfInterval(toDate32('2299-12-31') + INTERVAL 100000000 DAY, INTERVAL 2 WEEK)) = 2932892
    SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toDayOfWeek(toStartOfInterval(toDate32('2299-12-31') + INTERVAL 100000000 DAY, INTERVAL 2 WEEK)) = 1
    SETTINGS enable_extended_results_for_datetime_functions = 1;

-- A pre-epoch `Date32` value on the in-LUT `DAY` path must floor to the start of its interval, not round
-- toward zero (forward in time). `1969-12-22 .. 1969-12-31` are day numbers `-10 .. -1`, so a 10-day
-- interval must map all of them to the same (`-10`) bucket; before the fix `1969-12-31` truncated to `0`
-- (`1970-01-01`), a different bucket than `1969-12-22`. (The `DAY` result surfaces as epoch-based `DateTime`,
-- so the floored pre-epoch boundary is not directly representable; these bucket-equality invariants verify
-- the flooring independently of the surface value.)
SELECT toStartOfInterval(toDate32('1969-12-31'), INTERVAL 10 DAY) = toStartOfInterval(toDate32('1969-12-22'), INTERVAL 10 DAY);
SELECT toStartOfInterval(toDate32('1969-12-31'), INTERVAL 10 DAY) = toStartOfInterval(toDate32('1969-12-25'), INTERVAL 10 DAY);
-- The next interval up (`1970-01-01`, day `0`) must be a distinct bucket, i.e. the pre-epoch input did not
-- round forward into it.
SELECT toStartOfInterval(toDate32('1970-01-01'), INTERVAL 10 DAY) != toStartOfInterval(toDate32('1969-12-31'), INTERVAL 10 DAY);

-- Extreme `DateTime64` values combined with extreme interval counts reach the out-of-LUT-range rounding
-- branch, where the addition used to overflow. The result for such out-of-range values is
-- implementation-defined, so it is discarded; the test only requires that the evaluation does not overflow.
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 MONTH) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 MONTH) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 4611686018427387904 WEEK) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 WEEK) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 DAY) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 DAY) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(-9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 QUARTER) FORMAT Null;
SELECT toStartOfInterval(toDateTime64(9223372036854775807, 0, 'UTC'), INTERVAL 9223372036854775807 QUARTER) FORMAT Null;

SELECT 'ok';
