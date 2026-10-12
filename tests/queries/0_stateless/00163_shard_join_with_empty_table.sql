-- Tags: shard

SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;
SET join_algorithm = 'hash';
-- LEFT JOIN of `system.numbers` with an empty right table, then LIMIT 10, then ORDER BY.
-- With one thread the left scan yields 0-9. With several threads `hash` may probe in
-- parallel, so LIMIT after the join would pick an arbitrary 10 rows. Bound the left
-- side of the multithreaded queries with `LIMIT 10` (no `ORDER BY`: a sort of unbounded
-- `system.numbers` reads until `max_rows_to_read`). Empty-right LEFT JOIN still has to
-- scan the left (it cannot skip it); the bound is a test-side finite prefix.
--  - first run on a single thread to ensure it still works with SpillingHashJoin
--  - then on multiple threads with automatic spilling disabled
SET max_threads = 1;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.numbers))
    ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT dummy + 2 AS number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.one))
    ANY INNER JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.numbers))
    GLOBAL ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT dummy + 2 AS number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.one))
    GLOBAL ANY INNER JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;

SET max_threads = 6;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.numbers) LIMIT 10)
    ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT dummy + 2 AS number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.one))
    ANY INNER JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.numbers) LIMIT 10)
    GLOBAL ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n
) ORDER BY number;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT dummy + 2 AS number, number / 2 AS n FROM remote('127.0.0.{2,3}', system.one))
    GLOBAL ANY INNER JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 0)
    USING n LIMIT 10
) ORDER BY number;
