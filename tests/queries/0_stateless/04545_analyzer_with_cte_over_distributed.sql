-- Tags: shard
set enable_analyzer=1;

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;

CREATE TABLE t_local (x UInt64) ENGINE = Memory;
INSERT INTO t_local VALUES (1), (2), (3);
CREATE TABLE t_dist AS t_local ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t_local);

WITH ids AS (SELECT x FROM t_local WHERE x < 3)
SELECT count() FROM t_dist WHERE x IN (ids);

DROP TABLE t_dist;
DROP TABLE t_local;
