SET session_timezone = 'Europe/Amsterdam';

SELECT toStartOfInterval(CAST('2024-10-26', 'Date'), toIntervalMonth(1), CAST('2023-01-15', 'Date'));
