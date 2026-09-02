SELECT k
FROM (
     SELECT k, abs(v) AS _v
     FROM remote('127.{1,2}', view(select materialize('foo') as k, -1 as v))
     ORDER BY _v ASC
     LIMIT 1 BY k
)
GROUP BY k;

-- Simplified version of the reproducer provided in [1].
--   [1]: https://github.com/ClickHouse/ClickHouse/issues/37045
SELECT dummy
FROM remote('127.{1,2}', system.one)
WHERE dummy IN (SELECT 0)
LIMIT 1 BY dummy;
