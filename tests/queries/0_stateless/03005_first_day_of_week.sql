-- Testing behavior of date/time functions under setting 'first_day_of_week'.

SELECT '-- toStartOfInterval';

-- default behavior
SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK), -- Monday, Jan 01
    toStartOfInterval(dt, INTERVAL 2 WEEK); -- Monday, Dec 25

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK), -- Monday, Jan 01
    toStartOfInterval(dt, INTERVAL 2 WEEK) -- Monday, Dec 25
SETTINGS first_day_of_week = 'Monday';

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK), -- Sunday, Dec 31
    toStartOfInterval(dt, INTERVAL 2 WEEK) -- Sunday, Dec 24
SETTINGS first_day_of_week = 'Sunday';
