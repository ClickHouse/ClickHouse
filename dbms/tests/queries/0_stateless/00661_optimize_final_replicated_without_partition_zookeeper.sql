DROP TABLE IF EXISTS test.partitioned_by_tuple_replica1;
DROP TABLE IF EXISTS test.partitioned_by_tuple_replica2;
CREATE TABLE test.partitioned_by_tuple_replica1(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple', '1') PARTITION BY (d, x) ORDER BY (d, x, w);
CREATE TABLE test.partitioned_by_tuple_replica2(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple', '2') PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO test.partitioned_by_tuple_replica1 VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO test.partitioned_by_tuple_replica1 VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO test.partitioned_by_tuple_replica1 VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE TABLE test.partitioned_by_tuple_replica1;

SYSTEM SYNC REPLICA test.partitioned_by_tuple_replica2;
SELECT * FROM test.partitioned_by_tuple_replica2 ORDER BY d, x, w, y;

OPTIMIZE TABLE test.partitioned_by_tuple_replica1 FINAL;

SYSTEM SYNC REPLICA test.partitioned_by_tuple_replica2;
SELECT * FROM test.partitioned_by_tuple_replica2 ORDER BY d, x, w, y;

DROP TABLE test.partitioned_by_tuple_replica1;
DROP TABLE test.partitioned_by_tuple_replica2;
