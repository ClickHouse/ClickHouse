-- Tags: distributed

-- https://github.com/ClickHouse/ClickHouse/issues/77468

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS v0;

CREATE TABLE t0 (c0 Array(String)) ENGINE = Memory;
CREATE TABLE t1 (c0 Array(String)) ENGINE = Distributed('test_shard_localhost', currentDatabase(), t0);
CREATE MATERIALIZED VIEW v0 TO t1 (c0 String) AS (SELECT 1::Array(Int) AS c0); -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS v0;

CREATE TABLE t0 (c1 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c1 Date) ENGINE = Distributed('test_shard_localhost', currentDatabase(), t0);
CREATE MATERIALIZED VIEW v0 TO t1 (c1 String) AS (SELECT '2010-10-10' AS c1);
SELECT CAST(c1 AS Enum('1' = 1)) FROM v0;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS v0;
