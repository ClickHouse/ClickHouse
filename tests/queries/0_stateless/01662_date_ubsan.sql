-- { echo }
-- tests with INT64_MIN
SELECT addMinutes(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addHours(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
SELECT addWeeks(toDateTime('2021-01-01 00:00:00', 'GMT'), 9223372036854775808);
-- tests with inf
SELECT addMinutes(toDateTime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addHours(toDateTime('2021-01-01 00:00:00', 'GMT'), inf);
SELECT addWeeks(toDateTime('2021-01-01 00:00:00', 'GMT'), inf);
