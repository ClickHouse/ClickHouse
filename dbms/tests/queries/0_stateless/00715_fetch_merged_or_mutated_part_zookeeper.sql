DROP TABLE IF EXISTS test.fetches_r1;
DROP TABLE IF EXISTS test.fetches_r2;

CREATE TABLE test.fetches_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/fetches', 'r1') ORDER BY x;
CREATE TABLE test.fetches_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/fetches', 'r2') ORDER BY x
    SETTINGS prefer_fetch_merged_part_time_threshold=0,
             prefer_fetch_merged_part_size_threshold=0;

INSERT INTO test.fetches_r1 VALUES (1);
INSERT INTO test.fetches_r1 VALUES (2);
INSERT INTO test.fetches_r1 VALUES (3);

SYSTEM SYNC REPLICA test.fetches_r2;

DETACH TABLE test.fetches_r2;

SET replication_alter_partitions_sync=0;
OPTIMIZE TABLE test.fetches_r1 PARTITION tuple() FINAL;
SYSTEM SYNC REPLICA test.fetches_r1;

-- After attach replica r2 should fetch the merged part from r1.
ATTACH TABLE test.fetches_r2;
SYSTEM SYNC REPLICA test.fetches_r2;

SELECT '*** Check data after fetch of merged part ***';
SELECT _part, * FROM test.fetches_r2 ORDER BY x;

DETACH TABLE test.fetches_r2;

-- Add mutation that doesn't change data.
ALTER TABLE test.fetches_r1 DELETE WHERE x = 0;
SYSTEM SYNC REPLICA test.fetches_r1;

-- After attach replica r2 should compare checksums for mutated part and clone the local part.
ATTACH TABLE test.fetches_r2;
SYSTEM SYNC REPLICA test.fetches_r2;

SELECT '*** Check data after fetch/clone of mutated part ***';
SELECT _part, * FROM test.fetches_r2 ORDER BY x;

DROP TABLE test.fetches_r1;
DROP TABLE test.fetches_r2;
