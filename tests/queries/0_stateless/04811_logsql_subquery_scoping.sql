SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04811;
CREATE TABLE logs_04811
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `user_id` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04811 VALUES
    ('2024-01-01 00:00:00', 'error one', 'error', 'u1'),
    ('2024-01-01 06:00:00', 'info one', 'info', 'u1'),
    ('2024-01-01 12:00:00', 'info two', 'info', 'u2'),
    ('2024-01-02 12:00:00', 'late error', 'error', 'u1'),
    ('2024-01-02 18:00:00', 'late info', 'info', 'u3');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04811';
SET dialect = 'logsql';

-- The `_time` range of an `in(...)` subquery must not leak into the outer `rate()` denominator
-- (3 matching rows over the one-day outer range).
_time:[2024-01-01Z, 2024-01-02Z) user_id:in(_time:[2024-01-01Z, 2024-01-11Z) | fields user_id) | stats rate() as per_second;

-- A `global_filter` declared by a subquery applies inside the subquery only.
user_id:in(options(global_filter=(level:error)) * | fields user_id) | count();

-- A `time_offset` declared by a subquery must not shift the outer `_time` filters.
user_id:in(options(time_offset=24h) _time:[2024-01-02Z, 2024-01-03Z) | fields user_id) _time:[2024-01-02Z, 2024-01-03Z) | count();

SET dialect = 'clickhouse';
DROP TABLE logs_04811;
