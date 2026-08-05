SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04812;
CREATE TABLE logs_04812
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `size` String
) ENGINE = MergeTree ORDER BY _time;

-- 2024-01-01 is a Monday.
INSERT INTO logs_04812 VALUES
    ('2024-01-01 00:00:00', 'alpha', 'error', '5'),
    ('2024-01-01 10:30:00', 'bravo charlie', 'info', '30'),
    ('2024-01-01 20:00:00', 'delta', 'info', 'not-a-number');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04812';
SET dialect = 'logsql';

-- Numeric comparison filters interpret string fields numerically ("5" < 20 < "30");
-- values that are not numbers never match.
size:>=20 | fields _msg;
size:<20 | fields _msg;
size:range[4, 100] | fields _msg | sort by (_msg);

-- Computed fields overwrite an existing same-named column instead of duplicating it.
* | len(_msg) as level | fields _msg, level | sort by (_msg);
* | copy _msg as level | fields level | sort by (level);

-- The format pipe overwrites an existing field.
* | format if (level:error) "X" as _msg | fields _msg | sort by (_msg);

-- Writing into a field without a backing column is an explicit error, not a silent no-op.
* | format if (level:error) "X" as s1 | fields s1; -- { serverError NO_SUCH_COLUMN_IN_TABLE, BAD_ARGUMENTS }
* | extract if (level:error) "a<s2>b" | fields s2; -- { serverError NO_SUCH_COLUMN_IN_TABLE, BAD_ARGUMENTS }

-- Inverted day_range/week_range match nothing, as in VictoriaLogs.
_time:day_range[18:00, 08:00) | count();
_time:week_range[Fri, Mon] | count();

-- Equal endpoints: the inclusive range matches the boundary, the exclusive one is empty.
_time:day_range[00:00, 00:00] | count();
_time:day_range[00:00, 00:00) | count();
_time:week_range[Mon, Mon] | count();

SET dialect = 'clickhouse';
DROP TABLE logs_04812;
