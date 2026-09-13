SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04828;
CREATE TABLE logs_04828
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `service` String,
    `user_id` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04828 VALUES
    ('2024-01-01 00:00:00', 'web error', 'error', 'web', 'u1'),
    ('2024-01-01 01:00:00', 'web info', 'info', 'web', 'u2'),
    ('2024-01-01 02:00:00', 'api error', 'error', 'api', 'u3'),
    ('2024-01-01 03:00:00', 'api info', 'info', 'api', 'u4'),
    ('2024-01-01 04:00:00', 'other web error', 'error', 'web', 'u5'),
    ('2024-01-01 05:00:00', 'api error by u2', 'error', 'api', 'u2');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04828';
SET dialect = 'logsql';

-- A subquery inherits the outer `global_filter`; its own `global_filter` is ANDed with the
-- inherited one, so the subquery below sees `level:error AND service:web` and matches u1 and u5 only:
-- count is 2. If the subquery-local filter overwrote the inherited one, u2 (seen on a `web` info
-- row) would leak its `api` error row into the outer result, giving 3.
options(global_filter=(level:error)) user_id:in(options(global_filter=(service:web)) * | fields user_id) | count();

-- The outer query itself keeps only its own `global_filter`: all error rows match.
options(global_filter=(level:error)) * | count();

-- The subquery-local `global_filter` must not leak back into a sibling subquery.
options(global_filter=(level:error)) user_id:in(options(global_filter=(service:web)) * | fields user_id) or user_id:in(options(global_filter=(service:api)) * | fields user_id) | sort by (_time) | fields _msg;

-- Nested two levels deep: the innermost subquery sees the conjunction of all three filters.
options(global_filter=(level:error)) user_id:in(options(global_filter=(service:web)) user_id:in(options(global_filter=(user_id:u1)) * | fields user_id) | fields user_id) | count();

-- Repeated `global_filter` options within one query are ANDed as well.
options(global_filter=(level:error), global_filter=(service:api)) * | count();

SET dialect = 'clickhouse';
DROP TABLE logs_04828;
