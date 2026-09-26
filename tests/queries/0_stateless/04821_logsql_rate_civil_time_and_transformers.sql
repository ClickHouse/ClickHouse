SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04821;
CREATE TABLE logs_04821
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `size` String,
    `bytes` UInt64
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04821 VALUES
    ('2024-01-01 00:00:00', 'alpha', 'error', '5', 5),
    ('2024-01-01 10:30:00', 'bravo charlie', 'info', '30', 30),
    ('2024-01-01 20:00:00', 'delta', 'info', 'not-a-number', 7);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04821';
SET dialect = 'logsql';

-- rate() works for absolute `_time` ranges without an explicit timezone:
-- the range length is computed at execution time in the session timezone.
_time:[2024-01-01, 2024-01-02) | stats rate();
_time:2024-01-01 | stats rate();
_time:[2024-01-01, 2024-01-02) | stats rate_sum(bytes);

-- With an explicit timezone the length is a parse-time constant; same result.
_time:[2024-01-01Z, 2024-01-02Z) | stats rate();

-- The extract pipe overwrites an existing field instead of duplicating the column.
* | extract "<level>a" | fields _msg, level | sort by (_msg);

-- The `default` math operator catches all non-finite results: NaN and infinities.
* | math 1 / 0 default 42 as size | fields size | limit 1;
* | math -1 / 0 default 42 as size | fields size | limit 1;
* | math 0 / 0 default 7 as size | fields size | limit 1;
* | math 1 / 2 default 9 as size | fields size | limit 1;

-- hexnumencode always emits 16 uppercase hex digits; non-numbers keep the raw value.
* | format "<hexnumencode:size>" as _msg | fields _msg | sort by (_msg);

SET dialect = 'clickhouse';
DROP TABLE logs_04821;
