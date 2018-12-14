DROP TABLE IF EXISTS test.byte_identical_r1;
DROP TABLE IF EXISTS test.byte_identical_r2;

CREATE TABLE test.byte_identical_r1(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/byte_identical', 'r1') ORDER BY x;
CREATE TABLE test.byte_identical_r2(x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test/byte_identical', 'r2') ORDER BY x;

INSERT INTO test.byte_identical_r1(x) VALUES (1), (2), (3);
SYSTEM SYNC REPLICA test.byte_identical_r2;

-- Add a column with a default expression that will yield different values on different replicas.
-- Call optimize to materialize it. Replicas should compare checksums and restore consistency.
ALTER TABLE test.byte_identical_r1 ADD COLUMN y DEFAULT rand();
OPTIMIZE TABLE test.byte_identical_r1 PARTITION tuple() FINAL;

SELECT x, t1.y - t2.y FROM test.byte_identical_r1 t1 ANY INNER JOIN test.byte_identical_r2 t2 USING x ORDER BY x;

DROP TABLE test.byte_identical_r1;
DROP TABLE test.byte_identical_r2;
