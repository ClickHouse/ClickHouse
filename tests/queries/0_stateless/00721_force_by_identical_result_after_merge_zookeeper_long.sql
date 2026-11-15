-- Tags: long, zookeeper

SET prefer_warmed_unmerged_parts_seconds = 0;

DROP TABLE IF EXISTS byte_identical_r1;
DROP TABLE IF EXISTS byte_identical_r2;

CREATE TABLE byte_identical_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00721/byte_identical', 'r1') ORDER BY x;
CREATE TABLE byte_identical_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_00721/byte_identical', 'r2') ORDER BY x;

INSERT INTO byte_identical_r1(x) VALUES (1), (2), (3);
SYSTEM SYNC REPLICA byte_identical_r2;

-- Add a column with a default expression that will yield different values on different replicas.
-- Call optimize to materialize it. Replicas should compare checksums and restore consistency.
ALTER TABLE byte_identical_r1 ADD COLUMN y UInt64 DEFAULT rand();
SYSTEM SYNC REPLICA byte_identical_r1;
SYSTEM SYNC REPLICA byte_identical_r2;
SET replication_alter_partitions_sync=2;
OPTIMIZE TABLE byte_identical_r1 PARTITION tuple() FINAL;
SYSTEM SYNC REPLICA byte_identical_r2;

SELECT x, t1.y - t2.y FROM byte_identical_r1 t1 SEMI LEFT JOIN byte_identical_r2 t2 USING x ORDER BY x;

DROP TABLE byte_identical_r1;
DROP TABLE byte_identical_r2;
