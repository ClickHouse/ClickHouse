-- There is a bug in old analyzer with currentDatabase() and distributed query.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_local_1;
DROP TABLE IF EXISTS t_local_2;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_distr;

CREATE TABLE t_local_1 (a UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_local_2 (a UInt32) ENGINE = MergeTree ORDER BY  a;

INSERT INTO t_local_1 VALUES (1);
INSERT INTO t_local_2 VALUES (2);

CREATE TABLE t_merge AS t_local_1 ENGINE = Merge(currentDatabase(), '^(t_local_1|t_local_2)$');
CREATE TABLE t_distr AS t_local_1 ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_merge, rand());

SELECT a, _table FROM t_merge ORDER BY a;
SELECT a, _table FROM t_distr ORDER BY a;

SELECT a, _database = currentDatabase() FROM t_merge ORDER BY a;
SELECT a, _database = currentDatabase() FROM t_distr ORDER BY a;

DROP TABLE IF EXISTS t_local_1;
DROP TABLE IF EXISTS t_local_2;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_distr;
