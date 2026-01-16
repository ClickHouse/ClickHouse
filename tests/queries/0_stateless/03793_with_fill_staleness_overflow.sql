-- { echo }
SELECT arrayJoin([toDateTime('2026-01-01 00:00:00', 'UTC'), toTimeZone(fromUnixTimestamp(0), 'UTC')]) a ORDER BY a DESC WITH FILL STALENESS toIntervalSecond(-2);
