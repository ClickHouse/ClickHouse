DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_distr;

CREATE TABLE t_local (a Int) ENGINE = Memory;
CREATE TABLE t_distr (a Int) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_local');

INSERT INTO t_local VALUES (1), (2);
SET optimize_skip_unused_shards = 1;
SELECT * FROM t_distr WHERE a = 1;

DROP table t_local;
DROP table t_distr;
