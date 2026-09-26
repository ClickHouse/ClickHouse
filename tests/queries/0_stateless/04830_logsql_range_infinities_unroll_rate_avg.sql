SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_04830;
CREATE TABLE logs_04830
(
    `_time` DateTime,
    `_msg` String,
    `num` String,
    `size` UInt64,
    `a` Nullable(Float64),
    `b` Nullable(Float64),
    `tags` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04830 VALUES
    ('2024-01-01 00:00:00', 'first', '0.12345678901234567890123456789012345678901234567890', 5, NULL, 10, '["a","b"]'),
    ('2024-01-01 01:00:00', 'second', '0.12345678901234567890123456789012345678901234567891', 15, 2, NULL, '["c"]'),
    ('2024-01-01 02:00:00', 'third', '100', 20, NULL, NULL, '[]');

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04830';
SET dialect = 'logsql';

-- Exact comparisons distinguish values that differ only after the 38th fractional digit:
-- the decimal scale is widened to the exact fractional length of the literal.
num:=0.12345678901234567890123456789012345678901234567890 | count();
num:=0.12345678901234567890123456789012345678901234567891 | count();
num:>0.12345678901234567890123456789012345678901234567890 | count();

-- A literal that does not fit even the full Decimal256 precision (77 fractional digits)
-- falls back to the lossy Float64 comparison instead of silently truncating.
num:=0.11111111111111111111111111111111111111111111111111111111111111111111111111111 | count();

-- The sign of an infinite range() endpoint matters: wrong-way infinities make an empty
-- interval, right-way ones mean "unbounded".
size:range(10, -inf) | count();
size:range[inf, 10] | count();
size:range(-inf, 10] | count();
size:range[10, inf) | count();
-size:range(10, -inf) | count();

-- len_range() rejects negative bounds and treats infinities like range().
_msg:len_range(-1, 3) | count(); -- { error SYNTAX_ERROR }
_msg:len_range(1, -3) | count(); -- { error SYNTAX_ERROR }
_msg:len_range(10, -inf) | count();
_msg:len_range(inf, 10) | count();
_msg:len_range(0, inf) | count();
_msg:len_range(6, inf) | count();

-- limit / offset before unroll count the source rows, not the unrolled rows.
* | sort by (_time) | limit 1 | unroll(tags) | fields tags;
* | sort by (_time) | offset 1 | limit 1 | unroll(tags) | fields tags;

-- An empty civil-time window (carried as runtime expressions) falls back to the plain
-- count, like the parse-time branch, instead of dividing by zero.
_time:>=2024-01-01T00:00:00 _time:<2024-01-01T00:00:00 | stats rate();
-- A non-empty civil-time window still divides by its length (2 rows over 2 hours).
_time:>=2024-01-01T00:00:00 _time:<2024-01-01T02:00:00 | stats rate();

-- Pooled avg() counts only the present values: one value of `a` and one of `b`.
* | stats avg(a, b);
-- With no present values at all the average is NULL.
third | stats avg(a, b);
-- Non-Nullable columns pool all row values.
* | stats avg(size, size);

SET dialect = 'clickhouse';
DROP TABLE logs_04830;
