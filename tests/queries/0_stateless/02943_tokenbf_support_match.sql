-- Tags: no-parallel

DROP DATABASE IF EXISTS test_tokenbf_match;

CREATE DATABASE test_tokenbf_match;

CREATE TABLE test_tokenbf_match.test_tokenbf 
(
    `id` UInt32,
    `str` String,
    INDEX str_idx str TYPE tokenbf_v1(256, 2, 0) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;
 
INSERT INTO test_tokenbf_match.test_tokenbf VALUES (1, 'Hello ClickHouse'), (2, 'Hello World'), (3, 'Hello Github'), (4, 'Hello Cloud'), (5, 'OLAP Database');

--SKIP 3 GRANUS
--Required String: Hello
--Alternative String: Hello ClickHouse
--Alternative String: Hello World
SELECT 
  *
FROM
(
    EXPLAIN indexes=1
    SELECT * FROM test_tokenbf_match.test_tokenbf WHERE match(str, 'Hello (ClickHouse|World)')
)
WHERE
  explain like '%Granules%';


SELECT '';
SELECT '';


--SKIP 3 GRANUS
--No Required String
--Alternative String: ClickHouse
--Alternative String: World
SELECT
  *
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM test_tokenbf_match.test_tokenbf where match(str, '(.*?)* (ClickHouse|World)')
)
WHERE
  explain like '%Granules%';

SELECT '';
SELECT '';

--SKIP 4 GRANUS
--Required String: OLAP
--No Alternative String
SELECT
  *
FROM
(
    EXPLAIN indexes = 1
    SELECT * FROM test_tokenbf_match.test_tokenbf where match(str, 'OLAP (.*?)*')
)
WHERE
  explain like '%Granules%';

DROP DATABASE IF EXISTS test_tokenbf_match;
