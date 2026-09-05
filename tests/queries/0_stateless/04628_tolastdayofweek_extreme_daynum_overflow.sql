-- Regression test for the signed integer overflow in the sunday-first `week_mode` overloads of
-- `DateLUTImpl::toFirstDayNumOfWeek` / `toLastDayNumOfWeek` with an out-of-LUT-range day number.

-- Normal values must be unchanged, in every week mode, for Date, Date32 and DateTime.
SELECT toStartOfWeek(toDate('2021-06-22'), 0), toLastDayOfWeek(toDate('2021-06-22'), 0);
SELECT toStartOfWeek(toDate('2021-06-22'), 1), toLastDayOfWeek(toDate('2021-06-22'), 1);
SELECT toStartOfWeek(toDate('2021-06-22'), 2), toLastDayOfWeek(toDate('2021-06-22'), 2);
SELECT toStartOfWeek(toDate('2021-06-22'), 3), toLastDayOfWeek(toDate('2021-06-22'), 3);
SELECT toStartOfWeek(toDateTime('2021-06-22 12:00:00'), 0), toLastDayOfWeek(toDateTime('2021-06-22 12:00:00'), 0);
SELECT toStartOfWeek(toDate32('2021-06-22'), 0), toLastDayOfWeek(toDate32('2021-06-22'), 0) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toStartOfWeek(toDate32('2021-06-22'), 1), toLastDayOfWeek(toDate32('2021-06-22'), 1) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toStartOfWeek(toDate32('1969-12-31'), 0), toLastDayOfWeek(toDate32('1969-12-31'), 0) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toStartOfWeek(toDate32('1969-12-31'), 1), toLastDayOfWeek(toDate32('1969-12-31'), 1) SETTINGS enable_extended_results_for_datetime_functions = 1;

-- A wrapping `addDays` produces a day number far outside the lookup table, up to INT32_MAX / INT32_MIN.
-- The extended results are raw day numbers, so an overflow is directly observable.
SELECT toInt32(toDate32('1970-01-01') + INTERVAL 2147483647 DAY), toInt32(toDate32('1970-01-01') - INTERVAL 2147483647 DAY) SETTINGS enable_extended_results_for_datetime_functions = 1;

-- The result must be a day near the end (resp. the start) of the representable calendar, whatever the week mode,
-- instead of the wrapped garbage the overflow used to produce. It stays consistent with the monday-first
-- overload (mode 1), which may itself land up to 6 days past the calendar boundary.
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 1)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 1)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 2)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 2)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 3)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 3)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toStartOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 1)), toInt32(toLastDayOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 1)) SETTINGS enable_extended_results_for_datetime_functions = 1;

-- Like the monday-first overload, the week boundary may land a few days past the representable calendar, where
-- `toDayOfWeek` reports the weekday of the clamped boundary day instead. Only a result inside the calendar
-- carries its own weekday: the first day of a sunday-first week is a Sunday (7).
SELECT toDayOfWeek(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)), toDayOfWeek(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toDayOfWeek(toStartOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)), toDayOfWeek(toLastDayOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;

-- The last day of a week is always 6 days after its first day.
SELECT toInt32(toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)) - toInt32(toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT toInt32(toLastDayOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)) - toInt32(toStartOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0)) SETTINGS enable_extended_results_for_datetime_functions = 1;

-- Without extended results the day number is clamped into `Date`, so an extreme day number saturates.
SELECT toStartOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0), toLastDayOfWeek(toDate32('1970-01-01') + INTERVAL 2147483647 DAY, 0);
SELECT toStartOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0), toLastDayOfWeek(toDate32('1970-01-01') - INTERVAL 2147483647 DAY, 0);
