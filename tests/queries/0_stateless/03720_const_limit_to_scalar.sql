-- Tags: distributed

-- https://github.com/ClickHouse/ClickHouse/issues/89607

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't0');

SELECT c0 FROM t1 OFFSET -10 ROWS SETTINGS optimize_const_name_size = 1;
SELECT c0 FROM t1 OFFSET 10 ROWS SETTINGS optimize_const_name_size = 1;
SELECT c0 FROM t1 LIMIT -10 SETTINGS optimize_const_name_size = 1;
SELECT c0 FROM t1 LIMIT 10 SETTINGS optimize_const_name_size = 1;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
