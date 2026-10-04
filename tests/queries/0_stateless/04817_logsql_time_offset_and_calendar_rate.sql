SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04817;
CREATE TABLE logs_04817
(
    `_time` DateTime,
    `_msg` String,
    `size` String
) ENGINE = MergeTree ORDER BY _time;

-- 2024-01-01 is a Monday, 2024-01-06 is a Saturday.
INSERT INTO logs_04817 VALUES
    ('2024-01-01 06:00:00', 'monday-morning', '5'),
    ('2024-01-01 22:00:00', 'monday-night', '7'),
    ('2024-01-06 12:00:00', 'saturday-noon', '11');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04817';
SET dialect = 'logsql';

-- options(time_offset=...) shifts day_range the same way as the other _time filters:
-- the window is moved into the past, so a row matches when its _time plus the offset falls into the range.
_time:day_range[05:00, 07:00) | fields _msg;
options(time_offset=4h) _time:day_range[09:00, 11:00) | fields _msg;
options(time_offset=4h) _time:day_range[05:00, 07:00) | count();

-- The same for week_range, and the local `offset` clause combines with the global one.
_time:week_range[Sat, Sat] | fields _msg;
options(time_offset=24h) _time:week_range[Sun, Sun] | fields _msg;
options(time_offset=12h) _time:week_range[Sun, Sun] offset 12h | fields _msg;

-- rate() and rate_sum() over fixed-length buckets work.
_time:[2024-01-01, 2024-01-02) | stats by (_time:hour) rate() | sort by (_time) | fields "rate()";

-- Month and year buckets have variable lengths, so a constant denominator would be wrong; they are rejected.
* | stats by (_time:month) rate(); -- { error NOT_IMPLEMENTED }
* | stats by (_time:year) rate_sum(size); -- { error NOT_IMPLEMENTED }

-- Other stats functions over calendar buckets still work.
* | stats by (_time:year) count() | fields "count(*)";

SET dialect = 'clickhouse';
DROP TABLE logs_04817;
