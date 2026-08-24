DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS d;

CREATE TABLE t (a String, b Int) ENGINE = TinyLog;
INSERT INTO t VALUES ('a', 0), ('a', 1), ('b', 0);
SELECT * FROM t;

SELECT '---';
CREATE TABLE d (a String, b Int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
SELECT DISTINCT b FROM (SELECT a, b FROM d GROUP BY a, b) order by b;
DROP TABLE d;

SELECT '---';
CREATE TABLE d (a String, b Int) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t);
SELECT DISTINCT b FROM (SELECT a, b FROM d GROUP BY a, b) order by b;
DROP TABLE d;

DROP TABLE t;
