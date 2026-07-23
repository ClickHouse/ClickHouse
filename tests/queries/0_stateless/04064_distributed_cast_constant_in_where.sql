-- Tags: no-fasttest
-- Tag no-fasttest: needs s2
-- https://github.com/ClickHouse/ClickHouse/issues/71830
-- The analyzer could remove CAST from constants when rewriting distributed queries,
-- causing type errors on remote shards.

SELECT s2RectUnion(number + 1, toUInt64(1), toUInt64(1), toUInt64(1)) AS c
FROM remote('127.0.0.{1,2}', numbers(1))
WHERE (c.1) IN (1);
