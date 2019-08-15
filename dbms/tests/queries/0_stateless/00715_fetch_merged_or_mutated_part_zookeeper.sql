DROP TABLE IF EXISTS fetches_r1;
DROP TABLE IF EXISTS fetches_r2;

CREATE TABLE fetches_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/fetches', 'r1') ORDER BY x;
CREATE TABLE fetches_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/fetches', 'r2') ORDER BY x
    SETTINGS prefer_fetch_merged_part_time_threshold=0,
             prefer_fetch_merged_part_size_threshold=0;

INSERT INTO fetches_r1 VALUES (1);
INSERT INTO fetches_r1 VALUES (2);
INSERT INTO fetches_r1 VALUES (3);

SYSTEM SYNC REPLICA fetches_r2;

DETACH TABLE fetches_r2;

SET replication_alter_partitions_sync=0;
OPTIMIZE TABLE fetches_r1 PARTITION tuple() FINAL;
SYSTEM SYNC REPLICA fetches_r1;

-- After attach replica r2 should fetch the merged part from r1.
ATTACH TABLE fetches_r2;
SYSTEM SYNC REPLICA fetches_r2;

SELECT '*** Check data after fetch of merged part ***';
SELECT _part, * FROM fetches_r2 ORDER BY x;

DETACH TABLE fetches_r2;

-- Add mutation that doesn't change data.
ALTER TABLE fetches_r1 DELETE WHERE x = 0;
SYSTEM SYNC REPLICA fetches_r1;

-- After attach replica r2 should compare checksums for mutated part and clone the local part.
ATTACH TABLE fetches_r2;
SYSTEM SYNC REPLICA fetches_r2;

SELECT '*** Check data after fetch/clone of mutated part ***';
SELECT _part, * FROM fetches_r2 ORDER BY x;

DROP TABLE fetches_r1;
DROP TABLE fetches_r2;
