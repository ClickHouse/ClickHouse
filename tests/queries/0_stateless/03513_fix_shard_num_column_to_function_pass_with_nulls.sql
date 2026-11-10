SET join_use_nulls = 1;
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 Int) ENGINE = Memory();
INSERT INTO t0 SELECT number FROM numbers(3);
CREATE TABLE t1 (c0 Int) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), t0);
SELECT t1._shard_num % t1._shard_num FROM t1 FULL JOIN (SELECT 1 AS c0) tx ON TRUE;
SELECT t1._shard_num % shardNum() FROM t1 FULL JOIN (SELECT 1 AS c0) tx ON TRUE;

-- This doesn't work, but it is a separate bug https://github.com/ClickHouse/ClickHouse/issues/80691
-- SELECT t1._shard_num % t1._shard_num FROM t1 FULL JOIN (SELECT 1 AS c0) tx ON TRUE ORDER BY t1._shard_num;
