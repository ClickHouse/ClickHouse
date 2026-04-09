DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t0;
DROP VIEW IF EXISTS v0;
CREATE TABLE t1 (c0 Int,c1 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t0 (c0 Int,c1 Int) ENGINE = Distributed('test_shard_localhost', currentDatabase(), t1, c0);
CREATE MATERIALIZED VIEW v0 TO t0 (c0 LowCardinality(Int),c1 LowCardinality(Int)) AS (SELECT 1 AS c0, 1 AS c1);
SELECT c0::Int FROM v0;
DROP VIEW v0;
DROP TABLE t0;
DROP TABLE t1;

