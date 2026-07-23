-- { echo }
SELECT arrayJoin([toDateTime('2026-01-01 00:00:00', 'UTC'), toTimeZone(fromUnixTimestamp(0), 'UTC')]) a ORDER BY a DESC WITH FILL STALENESS toIntervalSecond(-2);
SELECT arrayJoin([toTimeZone(fromUnixTimestamp(10), 'UTC'), toTimeZone(fromUnixTimestamp(0), 'UTC')]) a ORDER BY a DESC WITH FILL STALENESS toIntervalSecond(-2);
-- no overflow
SELECT arrayJoin([toTimeZone(fromUnixTimestamp(10), 'UTC'), toTimeZone(fromUnixTimestamp(2), 'UTC')]) a ORDER BY a DESC WITH FILL STALENESS toIntervalSecond(-2);
