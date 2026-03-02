-- Tags: distributed

-- Logical error: Chunk info was not set for chunk in MergingAggregatedTransform.
-- https://github.com/ClickHouse/ClickHouse/issues/97564

DROP TABLE IF EXISTS test_local_1;
DROP TABLE IF EXISTS test_local_2;
DROP TABLE IF EXISTS test_distributed_1;
DROP TABLE IF EXISTS test_distributed_2;

CREATE TABLE test_local_1 (date Date, value UInt32) ENGINE = MergeTree ORDER BY date;
CREATE TABLE test_local_2 (date Date, value UInt32) ENGINE = MergeTree ORDER BY date;
CREATE TABLE test_distributed_1 AS test_local_1 ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local_1, rand());
CREATE TABLE test_distributed_2 AS test_local_2 ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local_2, rand());

INSERT INTO test_local_1 VALUES ('2018-08-01', 100);
INSERT INTO test_local_2 VALUES ('2018-08-01', 200);

SELECT _table FROM merge(currentDatabase(), 'test_distributed_1|test_distributed_2') ARRAY JOIN [1] AS x GROUP BY _table ORDER BY _table;

DROP TABLE test_local_1;
DROP TABLE test_local_2;
DROP TABLE test_distributed_1;
DROP TABLE test_distributed_2;
