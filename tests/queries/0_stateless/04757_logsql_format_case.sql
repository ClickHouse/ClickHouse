-- Tags: no-fasttest
-- no-fasttest: the `uc:` and `lc:` format options use `upperUTF8` and `lowerUTF8`, which require ICU.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04757;
CREATE TABLE logs_04757
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `app` String,
    `size` UInt64
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04757 VALUES
    ('2024-01-01 10:00:00', 'request finished', 'warn', 'Web', 200),
    ('2024-01-01 10:01:00', 'привет мир', 'Ошибка', 'api', 512);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04757';
SET dialect = 'logsql';

-- uc: uppercases with Unicode awareness, lc: lowercases
level:=warn | format "app=<app> level=<uc:level> size=<size>" as summary | fields summary;
level:="Ошибка" | format "level=<lc:level> app=<uc:app>" as summary | fields summary;

SET dialect = 'clickhouse';
DROP TABLE logs_04757;
