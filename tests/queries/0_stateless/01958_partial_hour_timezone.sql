SET output_format_pretty_single_large_number_tip_threshold = 0;

-- Appeared in https://github.com/ClickHouse/ClickHouse/pull/26978#issuecomment-890889362
WITH toDateTime('1970-06-17 07:39:21', 'Africa/Monrovia') as t
SELECT toUnixTimestamp(t),
       timeZoneOffset(t),
       formatDateTime(t, '%F %T', 'Africa/Monrovia'),
       toString(t, 'Africa/Monrovia'),
       toStartOfMinute(t),
       toStartOfFiveMinutes(t),
       toStartOfFifteenMinutes(t),
       toStartOfTenMinutes(t),
       toStartOfHour(t),
       toStartOfDay(t),
       toStartOfWeek(t),
       toStartOfInterval(t, INTERVAL 1 second),
       toStartOfInterval(t, INTERVAL 1 minute),
       toStartOfInterval(t, INTERVAL 2 minute),
       toStartOfInterval(t, INTERVAL 5 minute),
       toStartOfInterval(t, INTERVAL 60 minute),
       addMinutes(t, 1),
       addMinutes(t, 60)
FORMAT Vertical;
