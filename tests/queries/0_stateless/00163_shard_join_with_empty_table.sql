-- Tags: shard

SET any_join_distinct_right_table_keys = 1;
SET joined_subquery_requires_alias = 0;
SET join_algorithm = 'hash';

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
