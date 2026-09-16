-- `timeSlot` on a DateTime64 near the Int64 minimum used to overflow and wrap around to a positive value.
SET enable_extended_results_for_datetime_functions = 1;

SELECT v, toInt64(timeSlot(reinterpret(v, 'DateTime64(0)')))
FROM values('v Int64', (-9223372036854775808), (-9223372036854775807), (-9223372036854774000), (-9223372036854773808), (-1801), (-1800), (-1799), (-1), (0), (1799), (1800))
ORDER BY v;
