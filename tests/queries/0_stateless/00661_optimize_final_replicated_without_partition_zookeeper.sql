-- Tags: replica

-- May affect part names
SET prefer_warmed_unmerged_parts_seconds=0;
SET ignore_cold_parts_seconds=0;

SET optimize_on_insert = 0;

DROP TABLE IF EXISTS partitioned_by_tuple_replica1_00661 SYNC;
DROP TABLE IF EXISTS partitioned_by_tuple_replica2_00661 SYNC;
CREATE TABLE partitioned_by_tuple_replica1_00661(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test/partitioned_by_tuple_00661', '1') PARTITION BY (d, x) ORDER BY (d, x, w) SETTINGS cache_populated_by_fetch=0;
CREATE TABLE partitioned_by_tuple_replica2_00661(d Date, x UInt8, w String, y UInt8) ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/test/partitioned_by_tuple_00661', '2') PARTITION BY (d, x) ORDER BY (d, x, w) SETTINGS cache_populated_by_fetch=0;

INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-02', 1, 'first', 3);
INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-01', 2, 'first', 2);
INSERT INTO partitioned_by_tuple_replica1_00661 VALUES ('2000-01-01', 1, 'first', 1), ('2000-01-01', 1, 'first', 2);

OPTIMIZE TABLE partitioned_by_tuple_replica1_00661;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2_00661;
SELECT * FROM partitioned_by_tuple_replica2_00661 ORDER BY d, x, w, y;

OPTIMIZE TABLE partitioned_by_tuple_replica1_00661 FINAL;

SYSTEM SYNC REPLICA partitioned_by_tuple_replica2_00661;
SELECT * FROM partitioned_by_tuple_replica2_00661 ORDER BY d, x, w, y;

DROP TABLE partitioned_by_tuple_replica1_00661 SYNC;
DROP TABLE partitioned_by_tuple_replica2_00661 SYNC;
