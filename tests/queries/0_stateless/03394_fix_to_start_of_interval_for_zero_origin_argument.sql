SET session_timezone = 'UTC';

SELECT toStartOfInterval(
    toDateTime64('2024-03-13 11:29:01.000000', 6, 'Europe/Rome'),
    INTERVAL 1 QUARTER,
    toDateTime64('1970-01-01 00:00:00.000', 6),
    'Europe/Rome'
);
