-- `timeSlot` used to overflow near the Int64 minimum and wrap around to a positive value.
SET enable_extended_results_for_datetime_functions = 1;

SELECT v, toInt64(timeSlot(reinterpret(v, 'DateTime64(0)')))
FROM values('v Int64', (-9223372036854775808), (-9223372036854775807), (-9223372036854774000), (-9223372036854773808), (-1801), (-1800), (-1799), (-1), (0), (1799), (1800))
ORDER BY v;

-- A scaled DateTime64 goes through the same rounding before the result is rebuilt from its components.
SELECT v, toInt64(timeSlot(reinterpret(v, 'DateTime64(3)')))
FROM values('v Int64', (-3600000), (-1800000), (0), (1800000))
ORDER BY v;

-- Near the `Int64` minimum the scaled result used to go below it when rebuilt from whole seconds.
SELECT v, toInt64(timeSlot(reinterpret(v, 'DateTime64(3)')))
FROM values('v Int64', (-9223372036854775808), (-9223372036853000000))
ORDER BY v;

SELECT v, toInt64(timeSlot(reinterpret(v, 'DateTime64(9)')))
FROM values('v Int64', (-9223372036854775808), (-1), (1800999999999))
ORDER BY v;

SELECT v, toUInt32(timeSlot(reinterpret(v, 'DateTime64(3)')))
FROM values('v Int64', (-9223372036854775808), (-1), (1800999), (9223372036854775807))
ORDER BY v
SETTINGS enable_extended_results_for_datetime_functions = 0;
