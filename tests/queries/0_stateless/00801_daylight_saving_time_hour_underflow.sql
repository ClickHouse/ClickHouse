-- See comment in DateLUTImpl.cpp: "We doesn't support cases when time change results in switching to previous day..."
SELECT
    ignore(toDateTime(370641600, 'Europe/Moscow') AS t),
    replaceRegexpAll(toString(t), '\\d', 'x'),
    toHour(t) < 24,
    replaceRegexpAll(formatDateTime(t, '%Y-%m-%d %H:%M:%S; %R:%S; %F %T'), '\\d', 'x');
