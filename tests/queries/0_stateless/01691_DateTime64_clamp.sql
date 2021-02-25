-- { echo }
SELECT toTimeZone(toDateTime(-2, 2), 'Europe/Moscow');
SELECT toDateTime64(-2, 2, 'Europe/Moscow');
SELECT CAST(-1 AS DateTime64(0, 'Europe/Moscow'));
SELECT CAST('2020-01-01 00:00:00.3' AS DateTime64(0, 'Europe/Moscow'));
SELECT toDateTime64(bitShiftLeft(toUInt64(1), 33), 2, 'Europe/Moscow') FORMAT Null;
SELECT toTimeZone(toDateTime(-2., 2), 'Europe/Moscow');
SELECT toDateTime64(-2., 2, 'Europe/Moscow');
SELECT toDateTime64(toFloat32(bitShiftLeft(toUInt64(1),33)), 2, 'Europe/Moscow');
SELECT toDateTime64(toFloat64(bitShiftLeft(toUInt64(1),33)), 2, 'Europe/Moscow') FORMAT Null;
