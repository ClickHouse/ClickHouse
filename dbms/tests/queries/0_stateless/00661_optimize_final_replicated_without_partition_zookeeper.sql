DROP TABLE IF EXISTS partitioned_by_tuple_replica1;
DROP TABLE IF EXISTS partitioned_by_tuple_replica2;
CREATE TABLE partitioned_by_tuple_replica1(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple', '1') PARTITION BY (d, x) ORDER BY (d, x, w);
CREATE TABLE partitioned_by_tuple_replica2(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/test/partitioned_by_tuple', '2') PARTITION BY (d, x) ORDER BY (d, x, w);

INSERT INTO partitioned_by_tuple_replica1 VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO partitioned_by_tuple_replica1 VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO partitioned_by_tuple_replica1 VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE TABLE partitioned_by_tuple_replica1;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2;
SELECT * FROM partitioned_by_tuple_replica2 ORDER BY d, x, w, y;

OPTIMIZE TABLE partitioned_by_tuple_replica1 FINAL;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2;
SELECT * FROM partitioned_by_tuple_replica2 ORDER BY d, x, w, y;

DROP TABLE partitioned_by_tuple_replica1;
DROP TABLE partitioned_by_tuple_replica2;
