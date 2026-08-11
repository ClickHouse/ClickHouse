DROP TABLE IF EXISTS logs_04848;
CREATE TABLE logs_04848
(
    `_time` DateTime,
    `_msg` String,
    `level` String,
    `app` String,
    `duration` String,
    `size` UInt64
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_04848 VALUES
    ('2024-01-01 00:00:00', 'request finished', 'info', 'web', '10', 5),
    ('2024-01-01 00:10:00', 'request failed', 'error', 'web', '2.5', 15),
    ('2024-01-01 00:20:00', 'cache miss', 'info', 'api', 'unknown', 100),
    ('2024-01-01 00:30:00', 'cache hit', 'debug', 'api', '', 1);

SET allow_experimental_logsql_dialect = 1;
SET logsql_table = 'logs_04848';
SET dialect = 'logsql';

-- The cases of switch(...) partition the rows: every row is counted into the first case it
-- matches, so the two overlapping cases below give 2 and 1 (not 2 and 2), and `default`
-- takes the rows matching neither.
* | stats count() switch(case (level:=info) as infos, case (app:=web) as webs, default as others);

-- The same for a switch over a non-count function: the `web` row with level `info` belongs
-- to the first case only.
* | stats sum(size) switch(case (level:=info) as info_size, case (app:=web) as web_size, default as other_size);

-- The order of the cases matters, unlike for the independent per-function `if` conditions.
* | stats count() switch(case (app:=web) as webs, case (level:=info) as infos, default as others);

-- `default` is the negation of all the explicit cases, wherever it is written.
* | stats count() switch(default as others, case (level:=info) as infos);

-- A case matching every row leaves nothing for the later cases.
* | stats count() switch(case (*) as all_rows, case (level:=info) as infos, default as others);

-- The numeric stats functions parse the numeric value out of a `String` field and skip the
-- values that are not numbers, instead of failing with a type error: only '10' and '2.5' count.
* | stats sum(duration) as s, avg(duration) as a, median(duration) as m, quantile(0.9, duration) as q, stddev(duration) as d;

-- The pooled form skips the non-numeric values in every field.
* | stats sum(duration, size) as s, avg(duration, size) as a;

-- `rate_sum` divides the same numeric sum by the length of the query time range.
_time:[2024-01-01T00:00:00Z, 2024-01-01T00:00:10Z) | stats rate_sum(duration) as r;

-- `running_stats`/`total_stats` sum() is numeric, too.
* | total_stats sum(duration) as total | fields total | limit 1;

-- A field with no numeric values at all sums to NULL, like a fully absent field.
* | stats sum(_msg) as s, avg(_msg) as a;

SET dialect = 'clickhouse';
DROP TABLE logs_04848;
