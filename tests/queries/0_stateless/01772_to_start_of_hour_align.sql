-- Rounding down to hour intervals is aligned to midnight even if the interval length does not divide the whole day.
SELECT toStartOfInterval(toDateTime('2021-03-23 03:58:00'), INTERVAL 11 HOUR);
SELECT toStartOfInterval(toDateTime('2021-03-23 13:58:00'), INTERVAL 11 HOUR);
SELECT toStartOfInterval(toDateTime('2021-03-23 23:58:00'), INTERVAL 11 HOUR);

-- It should work correctly even in timezones with non-whole hours offset. India have +05:30.
SELECT toStartOfHour(toDateTime('2021-03-23 13:58:00', 'Asia/Kolkata'));
SELECT toStartOfInterval(toDateTime('2021-03-23 13:58:00', 'Asia/Kolkata'), INTERVAL 6 HOUR);

-- Specifying the interval longer than 24 hours is not correct, but it works as expected by just rounding to midnight.
SELECT toStartOfInterval(toDateTime('2021-03-23 13:58:00', 'Asia/Kolkata'), INTERVAL 66 HOUR);

-- In case of timezone shifts, rounding is performed to the hour number on "wall clock" time.
-- The intervals may become shorter or longer due to time shifts. For example, the three hour interval may actually last two hours.
-- If the same hour number on "wall clock" time correspond to multiple time points due to shifting backwards, the unspecified time point is selected among the candidates.
SELECT toDateTime('2010-03-28 00:00:00', 'Europe/Moscow') + INTERVAL 15 * number MINUTE AS src, toStartOfInterval(src, INTERVAL 2 HOUR) AS rounded, toUnixTimestamp(src) AS t FROM numbers(20);
SELECT toDateTime('2010-10-31 00:00:00', 'Europe/Moscow') + INTERVAL 15 * number MINUTE AS src, toStartOfInterval(src, INTERVAL 2 HOUR) AS rounded, toUnixTimestamp(src) AS t FROM numbers(20);

-- And this should work even for non whole number of hours shifts.
SELECT toDateTime('2020-04-05 00:00:00', 'Australia/Lord_Howe') + INTERVAL 15 * number MINUTE AS src, toStartOfInterval(src, INTERVAL 2 HOUR) AS rounded, toUnixTimestamp(src) AS t FROM numbers(20);
SELECT toDateTime('2020-10-04 00:00:00', 'Australia/Lord_Howe') + INTERVAL 15 * number MINUTE AS src, toStartOfInterval(src, INTERVAL 2 HOUR) AS rounded, toUnixTimestamp(src) AS t FROM numbers(20);
