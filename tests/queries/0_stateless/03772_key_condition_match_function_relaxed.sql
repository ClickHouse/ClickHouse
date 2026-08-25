-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.


DROP TABLE IF EXISTS 03772_table_match;

CREATE TABLE 03772_table_match
ENGINE = MergeTree()
ORDER BY url AS
SELECT 'http://example1.com/' AS url;

-- { echo }
SELECT count(*)
FROM 03772_table_match
WHERE NOT match(url, '^https?://clickhouse[.]com/');

EXPLAIN indexes = 1
SELECT count(*)
FROM 03772_table_match
WHERE NOT match(url, '^https?://clickhouse[.]com/');

SELECT count(*)
FROM 03772_table_match
WHERE NOT match(url, '^abcd');

EXPLAIN indexes = 1
SELECT count(*)
FROM 03772_table_match
WHERE NOT match(url, '^abcd');

SELECT count(*)
FROM 03772_table_match
WHERE match(url, '^abcd');

EXPLAIN indexes = 1
SELECT count(*)
FROM 03772_table_match
WHERE match(url, '^abcd');

SELECT count(*)
FROM 03772_table_match
WHERE match(url, '^https?://clickhouse[.]com/') = false;

EXPLAIN indexes = 1
SELECT count(*)
FROM 03772_table_match
WHERE match(url, '^https?://clickhouse[.]com/') = false;
