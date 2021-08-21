-- We add 1, because function toString has special behaviour for zero datetime
WITH toDateTime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %T') != toString(t);
WITH toDateTime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%Y-%m-%d %H:%M:%S') != toString(t);
WITH toDateTime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%Y-%m-%d %R:%S') != toString(t);
WITH toDateTime(1 + rand() % 0xFFFFFFFF) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %R:%S') != toString(t);

WITH toDate(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F') != toString(t);

-- Note: in some other timezones, daylight saving time change happens in midnight, so the first time of day is 01:00:00 instead of 00:00:00.
-- Stick to Moscow timezone to avoid this issue.
WITH toDate(today() + rand() % 4096) AS t SELECT count() FROM numbers(1000000) WHERE formatDateTime(t, '%F %T', 'Europe/Moscow') != toString(toDateTime(t, 'Europe/Moscow'));
