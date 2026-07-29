-- The raw day number of a Date32 can be any Int32 (e.g. after unchecked arithmetic wrapped it around),
-- and toStartOfWeek/toLastDayOfWeek with a Sunday-first week mode used to overflow the signed day number
-- near the bounds of Int32.

-- toDate32('1900-01-01') has the raw day number -25567; the subtractions wrap the raw value
-- around to the minimum and the maximum of Int32 respectively.
SELECT (toDate32('1900-01-01') - 2147458081)::Int32, (toDate32('1900-01-01') - 2147458082)::Int32;

SELECT toStartOfWeek(toDate32('1900-01-01') - 2147458081, 0, 'UTC');
SELECT toStartOfWeek(materialize(toDate32('1900-01-01') - 2147458081), 0, 'UTC');
SELECT toStartOfWeek(toDate32('1900-01-01') - 2147458082, 0, 'UTC');
SELECT toStartOfWeek(materialize(toDate32('1900-01-01') - 2147458082), 0, 'UTC');

SELECT toLastDayOfWeek(toDate32('1900-01-01') - 2147458081, 0, 'UTC');
SELECT toLastDayOfWeek(materialize(toDate32('1900-01-01') - 2147458081), 0, 'UTC');
SELECT toLastDayOfWeek(toDate32('1900-01-01') - 2147458082, 0, 'UTC');
SELECT toLastDayOfWeek(materialize(toDate32('1900-01-01') - 2147458082), 0, 'UTC');
