SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04822;
CREATE TABLE logs_04822
(
    `_time` DateTime64(9, 'UTC'),
    `_msg` String,
    `size` String,
    `num` UInt64,
    `app` String,
    `lvl` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04822 VALUES
    ('2024-01-01 00:00:00', 'a', '20000000', 9007199254740993, 'web api', 'info'),
    ('2024-01-01 06:00:00', 'b', '5000000', 9007199254740992, 'quo"te\\back', 'a=b'),
    ('2024-01-01 12:00:00', 'c', 'not_a_number', 42, 'plain', ''),
    ('2024-01-01 18:00:00', 'd', '16', 16, 'x', 'y');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04822';
SET dialect = 'logsql';

-- Numeric comparison and equality literals use the same rich grammar as range():
-- byte sizes, base prefixes, underscores, and infinity, over a String-backed field.
size:>10.5M | fields _msg;
size:=0x10 | fields _msg;
size:>1_000 | fields _msg | sort by (_msg);
size:<inf | fields _msg | sort by (_msg);

-- Typed numeric columns are compared exactly: values above 2^53 that a Float64
-- cast would round together stay distinguishable.
num:>9007199254740992 | fields _msg;
num:<9007199254740993 | fields _msg | sort by (_msg);
num:=9007199254740993 | fields _msg;
num:range(9007199254740992, inf) | fields _msg;

-- Invalid timestamps are rejected instead of being normalized into wrong epochs:
-- Unix epochs beyond the DateTime64(9) range and impossible civil dates.
_time:10000000000 | stats count(); -- { error SYNTAX_ERROR }
_time:99999999999999999999 | stats count(); -- { error SYNTAX_ERROR }
_time:2025-02-29Z | stats count(); -- { error SYNTAX_ERROR }
_time:2024-04-31+02:00 | stats count(); -- { error SYNTAX_ERROR }
_time:9999-01-01Z | stats count(); -- { error SYNTAX_ERROR }
_time:2024-02-29Z | stats count();
_time:2024-01-01T00:00:00Z | stats count();

-- rate() derives its denominator from a window written as a pair of `_time`
-- comparison filters, with an explicit timezone and without one.
_time:>=2024-01-01Z _time:<2024-01-02Z | stats rate();
_time:>=2024-01-01 _time:<2024-01-02 | stats rate();
_time:>=2024-01-01T06:00:00Z _time:<2024-01-01T18:00:00Z | stats rate_sum(num);

-- Several bounds of the same kind intersect into the most restrictive one.
_time:>=2024-01-01Z _time:>=2024-01-01T06:00:00Z _time:<2024-01-02Z | stats rate();

-- pack_logfmt quotes values containing spaces, quotes, backslashes, or '=',
-- so that the output is unambiguous logfmt and round-trips through unpack_logfmt.
* | pack_logfmt fields (app, lvl) as packed | fields packed | sort by (packed);
* | pack_logfmt fields (app, lvl) as packed
  | unpack_logfmt from packed fields (app, lvl) result_prefix 'u_'
  | fields u_app, u_lvl | sort by (u_app);

DROP TABLE logs_04822;

-- rate() over civil day buckets uses the real bucket length: the day of a DST
-- transition (2024-03-31 in Europe/Amsterdam is 23 hours) gets its own denominator.
SET dialect = 'clickhouse';
DROP TABLE IF EXISTS logs_dst_04822;
CREATE TABLE logs_dst_04822
(
    `_time` DateTime('Europe/Amsterdam'),
    `_msg` String
) ENGINE = MergeTree ORDER BY _time;
INSERT INTO logs_dst_04822 SELECT toDateTime('2024-03-30 00:00:00', 'Europe/Amsterdam') + number * 1380, 'm' FROM numbers(120);

SET logsql_table = 'logs_dst_04822';
SET dialect = 'logsql';
* | stats by (_time:day) count(), rate() | sort by (_time) | limit 2;

-- Week buckets are not fixed-length either, and their key does not keep
-- the timezone, so rate() over them is rejected.
* | stats by (_time:week) rate(); -- { error NOT_IMPLEMENTED }

DROP TABLE logs_dst_04822;
