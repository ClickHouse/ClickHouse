DROP TABLE IF EXISTS foo_local;
DROP TABLE IF EXISTS foo_distributed;

CREATE TABLE foo_local (bar UInt64)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE TABLE foo_distributed AS foo_local
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), foo_local);

CREATE TEMPORARY TABLE _tmp_baz (qux UInt64);

SELECT * FROM foo_distributed JOIN _tmp_baz ON foo_distributed.bar = _tmp_baz.qux;

DROP TABLE foo_local;
DROP TABLE foo_distributed;
