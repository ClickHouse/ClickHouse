
SET max_threads = 1;

SELECT '-- Simple tests';
EXPLAIN PIPELINE
WITH t1 AS MATERIALIZED (SELECT number AS c FROM numbers(10)), t2 AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT * FROM t1
UNION ALL
SELECT * FROM t2;

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT *
FROM numbers(100) AS t
ANY LEFT JOIN dict AS d1 ON t.number = dict.key;

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT number, joinGet(dict, 'value', number)
FROM numbers(10);

EXPLAIN PIPELINE
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10)) ENGINE = Set
SELECT * FROM numbers(100) WHERE number IN t;

SELECT '-- CTE is used for index analyzer';
CREATE TABLE tm (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO tm SELECT * FROM numbers(1000000);
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(100)) SELECT * FROM tm WHERE x IN (SELECT c FROM t WHERE c > 10) ORDER BY x SETTINGS force_primary_key = 1;
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(100)) ENGINE=Set SELECT * FROM tm WHERE x IN (t) ORDER BY x SETTINGS force_primary_key = 1;
DROP TABLE tm;

SELECT '-- Distributed queries';
WITH t AS MATERIALIZED (SELECT number FROM numbers(100))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (SELECT * FROM t WHERE number > 5) ORDER BY number;

WITH t AS MATERIALIZED (SELECT number FROM numbers(100))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (t) ORDER BY number;

SELECT '-- Distributed broadcast JOIN';
CREATE TABLE fact_local (key UInt64, value String) ENGINE = Memory;
INSERT INTO fact_local VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE fact_dist (key UInt64, value String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), fact_local);
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(3))
SELECT key, value FROM fact_dist INNER JOIN t ON fact_dist.key = t.c ORDER BY key, value;
DROP TABLE IF EXISTS fact_local;
DROP TABLE IF EXISTS fact_dist;

SELECT '-- Same test but for analyzer';
SELECT '-- Simple tests';
EXPLAIN PIPELINE
WITH t1 AS MATERIALIZED (SELECT number AS c FROM numbers(10)), t2 AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT * FROM t1
UNION ALL
SELECT * FROM t2;

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT *
FROM numbers(100) AS t
ANY LEFT JOIN dict AS d1 ON t.number = dict.key;

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT number, joinGet(dict, 'value', number)
FROM numbers(10);

EXPLAIN PIPELINE
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10)) ENGINE = Set
SELECT * FROM numbers(100) WHERE number IN t;

SELECT '-- CTE is used for index analyzer';
CREATE TABLE tm (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO tm SELECT * FROM numbers(1000000);
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(100)) SELECT * FROM tm WHERE x IN (SELECT c FROM t WHERE c > 10) ORDER BY x SETTINGS force_primary_key = 1;
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(100)) ENGINE=Set SELECT * FROM tm WHERE x IN (t) ORDER BY x SETTINGS force_primary_key = 1;
DROP TABLE tm;

SELECT '-- Distributed queries';
WITH t AS MATERIALIZED (SELECT number FROM numbers(100))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (SELECT * FROM t WHERE number > 5) ORDER BY number;

WITH t AS MATERIALIZED (SELECT number FROM numbers(100))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (t) ORDER BY number;

SELECT '-- Distributed broadcast JOIN';
CREATE TABLE fact_local (key UInt64, value String) ENGINE = Memory;
INSERT INTO fact_local VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE fact_dist (key UInt64, value String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), fact_local);
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(3))
SELECT key, value FROM fact_dist INNER JOIN t ON fact_dist.key = t.c ORDER BY key, value;
DROP TABLE IF EXISTS fact_local;
DROP TABLE IF EXISTS fact_dist;
