DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS foo2;
DROP TABLE IF EXISTS foo2_dist;
DROP TABLE IF EXISTS merge1;

CREATE TABLE foo (`Id` Int32, `Val` Int32) ENGINE = MergeTree ORDER BY Id;
INSERT INTO foo SELECT number, number FROM numbers(100);

CREATE TABLE foo2 (`Id` Int32, `Val` Int32) ENGINE = MergeTree ORDER BY Id;
INSERT INTO foo2 SELECT number, number FROM numbers(100);
CREATE TABLE foo2_dist (`Id` UInt32, `Val` String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), foo2);

CREATE TABLE merge1 AS foo ENGINE = Merge(currentDatabase(), '^(foo|foo2_dist)$');

EXPLAIN PIPELINE graph = 1, compact = 1 SELECT * FROM merge1 FORMAT Null;
EXPLAIN PIPELINE graph = 1, compact = 1 SELECT * FROM merge1 FORMAT Null SETTINGS enable_analyzer=1;
