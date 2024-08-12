SET enable_analyzer=1;

-- Illegal column String of first argument of function concatWithSeparator. Must be a constant String.
SELECT concatWithSeparator('a', 'b') GROUP BY 'a';
-- use-of-uninitialized-value
SELECT concatWithSeparator('|', 'a', concatWithSeparator('|', CAST('a', 'LowCardinality(String)'))) GROUP BY 'a';
SELECT concatWithSeparator('|', 'a', concatWithSeparator('|', CAST('x', 'LowCardinality(String)'))) GROUP BY 'a';
-- should be const like for the query w/o GROUP BY
select dumpColumnStructure('x') GROUP BY 'x';
select dumpColumnStructure('x');
-- from https://github.com/ClickHouse/ClickHouse/pull/60046
SELECT cityHash64('limit', _CAST(materialize('World'), 'LowCardinality(String)')) FROM system.one GROUP BY GROUPING SETS ('limit');

WITH (
        SELECT dummy AS x
        FROM system.one
    ) AS y
SELECT
    y,
    min(dummy)
FROM remote('127.0.0.{1,2}', system.one)
GROUP BY y;

WITH (
        SELECT dummy AS x
        FROM system.one
    ) AS y
SELECT
    y,
    min(dummy)
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY y;

CREATE TABLE ttt (hr DateTime, ts DateTime) ENGINE=Memory
as select '2000-01-01' d, d;

SELECT
    count(),
    now() AS c1
FROM remote('127.0.0.{1,2}', currentDatabase(), ttt)
GROUP BY c1 FORMAT Null;

SELECT
    count(),
    now() AS c1
FROM remote('127.0.0.{3,2}', currentDatabase(), ttt)
GROUP BY c1 FORMAT Null;

SELECT
    count(),
    now() AS c1
FROM remote('127.0.0.{1,2}', currentDatabase(), ttt)
GROUP BY c1 + 1 FORMAT Null;

SELECT
    count(),
    now() AS c1
FROM remote('127.0.0.{3,2}', currentDatabase(), ttt)
GROUP BY c1 + 1 FORMAT Null;

SELECT
  count(),
  tuple(nullIf(toDateTime(formatDateTime(hr, '%F %T', 'America/Los_Angeles'), 'America/Los_Angeles'), toDateTime(0)))  as c1,
  defaultValueOfArgumentType(toTimeZone(ts, 'America/Los_Angeles'))  as c2,
  formatDateTime(hr, '%F %T', 'America/Los_Angeles')  as c3
FROM   remote('127.0.0.{1,2}', currentDatabase(),  ttt)
GROUP BY c1, c2, c3 FORMAT Null;

SELECT
  count(),
  tuple(nullIf(toDateTime(formatDateTime(hr, '%F %T', 'America/Los_Angeles'), 'America/Los_Angeles'), toDateTime(0)))  as c1,
  defaultValueOfArgumentType(toTimeZone(ts, 'America/Los_Angeles'))  as c2,
  formatDateTime(hr, '%F %T', 'America/Los_Angeles')  as c3
FROM   remote('127.0.0.{3,2}', currentDatabase(),  ttt)
GROUP BY c1, c2, c3 FORMAT Null;
