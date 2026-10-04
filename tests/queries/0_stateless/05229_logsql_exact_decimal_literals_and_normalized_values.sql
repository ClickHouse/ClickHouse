-- Regressions for the exact handling of integral literals written with a fraction or an
-- exponent, for the word-boundary semantics of the subquery form of `contains_any` and
-- `contains_all`, and for the normalization of missing field values in the `pack_*` and
-- `row_*` serializers.

DROP TABLE IF EXISTS logs_05229;
CREATE TABLE logs_05229
(
    `_time` DateTime,
    `_msg` String,
    `num` UInt64,
    `needle` String,
    `note` Nullable(String)
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05229 VALUES
    ('2024-01-01 00:00:00', 'foo bar', 9007199254740992, 'foo', 'n1'),
    ('2024-01-01 00:00:01', 'foobar', 9007199254740993, 'foo', NULL),
    ('2024-01-01 00:00:02', 'baz', 9007199254740994, 'baz', '');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_05229';
SET dialect = 'logsql';

-- Above 2^53 a fractional or exponential spelling of an integer is compared exactly,
-- exactly like its plain spelling.
num:=9007199254740993 | fields num;
num:=9007199254740993.0 | fields num;
num:=9.007199254740993e15 | fields num;
num:range[9.007199254740993e15, 9.007199254740993e15] | fields num;
num:>9.007199254740993e15 | fields num;
num:>=9.007199254740993e15 | fields num | sort by (num);

-- `stats by` buckets are exact for an integral step in any spelling.
* | stats by (num:1) count() as c | sort by (num);
* | stats by (num:1e0) count() as c | sort by (num);
* | stats by (num:1.0) count() as c | sort by (num);

-- `limit`-style arguments accept an integral spelling whose exact value is recoverable
-- from its text, and reject a spelling that has already rounded.
* | limit 9007199254740993.0 | count();
* | limit 9.007199254740993e15 | count();
* | limit 10.5K | count();
* | limit 9007199254740993.5 | count(); -- { error SYNTAX_ERROR }
_msg:len_range(0, 9.007199254740993e15) | count();
_msg:len_range(0, 9007199254740993.5) | count(); -- { error SYNTAX_ERROR }

-- The subquery form of `contains_any` and `contains_all` matches word boundaries,
-- exactly like the literal form.
_msg:contains_any(foo) | fields _msg | sort by (_msg);
_msg:contains_any(needle:foo | fields needle) | fields _msg | sort by (_msg);
_msg:contains_all(foo) | fields _msg | sort by (_msg);
_msg:contains_all(needle:foo | fields needle) | fields _msg | sort by (_msg);

-- A missing field value is the empty LogsQL value, not the `\N` of a `Nullable` column.
* | pack_json fields (_msg, note) as packed | sort by (_time) | fields packed;
* | pack_logfmt fields (_msg, note) as packed | sort by (_time) | fields packed;
* | stats by (_msg) row_any(_msg, note) as row | sort by (_msg);

-- An unquoted default of the `coalesce` pipe is a LogsQL string, like every value the
-- pipe returns, instead of a number that has no common type with them.
* | coalesce(note, needle) default 0 as c | sort by (_time) | fields c;
_msg:baz | coalesce(note) default 0 as c | fields c;

SET dialect = 'clickhouse';
DROP TABLE logs_05229;
