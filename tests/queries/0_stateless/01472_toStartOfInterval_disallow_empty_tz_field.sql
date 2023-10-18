SELECT toStartOfDay(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfDay(toDateTime('2017-12-31 03:45:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfMonth(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfMonth(toDateTime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfQuarter(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfQuarter(toDateTime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfYear(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfYear(toDateTime('2017-12-31 00:00:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfTenMinutes(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfTenMinutes(toDateTime('2017-12-31 05:12:30', 'UTC'), 'UTC'); -- success

SELECT toStartOfFifteenMinutes(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfFifteenMinutes(toDateTime('2017-12-31 01:17:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfHour(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfHour(toDateTime('2017-12-31 01:59:00', 'UTC'), 'UTC'); -- success

SELECT toStartOfMinute(toDateTime('2017-12-31 00:00:00', 'UTC'), ''); -- {serverError 43}
SELECT toStartOfMinute(toDateTime('2017-12-31 00:01:30', 'UTC'), 'UTC'); -- success

-- special case - allow empty time_zone when using functions like today(), yesterday() etc.
SELECT toStartOfDay(today()) FORMAT Null; -- success
SELECT toStartOfDay(yesterday()) FORMAT Null; -- success
