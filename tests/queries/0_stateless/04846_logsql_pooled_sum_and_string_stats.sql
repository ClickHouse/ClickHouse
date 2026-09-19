DROP TABLE IF EXISTS logs_04846;
CREATE TABLE logs_04846
(
    `_time` DateTime,
    `_msg` String,
    `size` UInt64,
    `code` UInt16,
    `a` Nullable(Float64),
    `b` Nullable(Float64),
    `note` Nullable(String)
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04846 VALUES
    ('2024-01-01 00:00:00', 'first', 5, 200, NULL, 10, 'ok'),
    ('2024-01-01 01:00:00', 'second', 15, 404, 2, NULL, ''),
    ('2024-01-01 02:00:00', 'third', 100, 500, NULL, NULL, NULL);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04846';
SET dialect = 'logsql';

-- The pooled multi-field sum() must skip a field whose values are all NULL in the
-- filtered set instead of returning NULL: over the `second` row, sum(a) = 2 and
-- sum(b) is NULL, and the pooled result is 2.
second | stats sum(a, b) as s;

-- Present values of both fields are pooled together.
* | stats sum(a, b) as s;

-- With no present values at all, the pooled sum is NULL, like for a single field.
third | stats sum(a, b) as s;

-- count_empty() compares the string representation of the value, so it works on typed
-- columns; a numeric value is never empty.
* | stats count_empty(size) as c;

-- A NULL value counts as empty, like a missing field in VictoriaLogs, and so does an
-- empty string; both fields of a row must be empty to be counted.
* | stats count_empty(note) as c;
* | stats count_empty(note, size) as c;

-- sum_len() sums the lengths of the string representations of typed values:
-- '5' + '15' + '100' = 1 + 2 + 3 = 6, and '200' + '404' + '500' = 9 in total.
* | stats sum_len(size) as l;
* | stats sum_len(size, code) as l;

-- A NULL value has length 0: 'ok' + '' + NULL = 2.
* | stats sum_len(note) as l;

SET dialect = 'clickhouse';
DROP TABLE logs_04846;
