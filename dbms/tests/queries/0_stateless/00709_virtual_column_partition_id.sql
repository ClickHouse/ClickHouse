DROP TABLE IF EXISTS test.partition_id;

CREATE TABLE IF NOT EXISTS test.partition_id (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.partition_id VALUES (100, 1), (200, 2), (300, 3);

SELECT _partition_id FROM test.partition_id ORDER BY x;

DROP TABLE IF EXISTS test.partition_id;

