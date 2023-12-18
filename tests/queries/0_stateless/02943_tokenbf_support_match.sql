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

SELECT '========================================';
SELECT '| SKIP 3 GRANUS                        |';
SELECT '| Required String: Hello               |';
SELECT '| Alternative String: Hello ClickHouse |';
SELECT '| Alternative String: Hello World      |';
SELECT '========================================';

EXPLAIN indexes=1 SELECT * FROM test_tokenbf_match.test_tokenbf WHERE match(str, 'Hello (ClickHouse|World)');

SELECT '';
SELECT '';

SELECT '========================================';
SELECT '| SKIP 3 GRANUS                        |';
SELECT '| No Required String                   |';
SELECT '| Alternative String: ClickHouse       |';
SELECT '| Alternative String: World            |';
SELECT '========================================';

EXPLAIN indexes = 1 SELECT * FROM test_tokenbf_match.test_tokenbf where match(str, '(.*?)* (ClickHouse|World)');

SELECT '';
SELECT '';

SELECT '========================================';
SELECT '| SKIP 4 GRANUS                        |';
SELECT '| Required String: OLAP                |';
SELECT '| No Alternative String                |';
SELECT '========================================';

EXPLAIN indexes = 1 SELECT * FROM test_tokenbf_match.test_tokenbf where match(str, 'OLAP (.*?)*');

DROP DATABASE IF EXISTS test_tokenbf_match;
