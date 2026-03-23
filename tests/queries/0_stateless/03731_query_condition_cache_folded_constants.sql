SET prefer_localhost_replica = 0,
    use_query_condition_cache = 1;

DROP TABLE IF EXISTS 03731_data;

CREATE TABLE 03731_data( `key` UInt64 ) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 8192
AS
SELECT number FROM numbers(30000);

SELECT '-- First query run to populate query condition cache';
SELECT
    shardNum(),
    min(key),
    max(key),
    count()
FROM remote('127.0.0.{1,2}', currentDatabase(), 03731_data)
WHERE (key >= (shardNum() * 10000))
  AND (key < ((shardNum() * 10000) + 10000))
GROUP BY 1
ORDER BY 1 ASC;

SELECT '-- Second query run to assert that query condition cache doesnt affect results';
SELECT
    shardNum(),
    min(key),
    max(key),
    count()
FROM remote('127.0.0.{1,2}', currentDatabase(), 03731_data)
WHERE (key >= (shardNum() * 10000))
  AND (key < ((shardNum() * 10000) + 10000))
GROUP BY 1
ORDER BY 1 ASC;

DROP TABLE 03731_data;
