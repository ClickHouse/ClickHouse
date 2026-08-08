SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04824;
CREATE TABLE logs_04824
(
    `_time` DateTime64(9, 'UTC'),
    `_msg` String,
    `num` UInt64,
    `size` String
) ENGINE = MergeTree ORDER BY _time;

-- 2023-11-14 22:13:20 UTC is the Unix timestamp 1700000000.
INSERT INTO logs_04824 VALUES
    ('2023-11-14 22:13:20.000000000', 'a', 18446744073709551615, '20480'),
    ('2023-11-14 22:13:20.123000000', 'b', 18446744073709551614, '20KiB'),
    ('2023-11-14 22:13:20.123456000', 'c', 9007199254740993, '1h'),
    ('2023-11-14 22:13:20.999999999', 'd', 9007199254740992, '10.5');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04824';
SET dialect = 'logsql';

-- Typed comparisons stay exact across the full UInt64 range: adjacent values
-- near UInt64::max must not collapse through a Float64 cast.
num:=18446744073709551615 | fields _msg;
num:=18446744073709551614 | fields _msg;
num:>18446744073709551614 | fields _msg;
num:>=18446744073709551615 | fields _msg;
num:<18446744073709551615 | fields _msg | sort by (_msg);

-- Rich integer literal forms (base prefixes, underscores) are exact too:
-- both spell 9007199254740993, the first value a Float64 would round away.
num:=0x20000000000001 | fields _msg;
num:=9_007_199_254_740_993 | fields _msg;
num:>0x20000000000000 num:<18446744073709551614 | fields _msg;

-- range() bounds are exact as well.
num:range(18446744073709551614, inf] | fields _msg | sort by (_msg);
num:range[18446744073709551615, 18446744073709551615] | fields _msg;
num:range(9_007_199_254_740_992, 0x20000000000001] | fields _msg;

-- Unix timestamps denote the whole period of their implied precision:
-- seconds match the second, milliseconds the millisecond, microseconds
-- the microsecond, and nanoseconds a single instant.
_time:1700000000 | stats count(*) as rows_in_second;
_time:1700000000123 | fields _msg | sort by (_msg);
_time:1700000000999 | fields _msg;
_time:1700000000123456 | fields _msg;
_time:1700000000123000000 | fields _msg;
_time:1700000000123456789 | stats count(*) as rows_at_instant;

-- The String-value contract: per-row numeric parsing of String field values
-- understands plain numeric text only; LogsQL-only spellings stored in the
-- field ('20KiB', '1h') are treated as non-numeric and do not match.
size:>10KiB | fields _msg;
size:>10 | fields _msg | sort by (_msg);
size:range(10, 30000) | fields _msg | sort by (_msg);

DROP TABLE logs_04824;
