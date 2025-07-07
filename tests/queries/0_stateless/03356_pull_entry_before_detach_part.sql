-- Tags: no-fasttest, no-parallel

-- Forbid fault injection to avoid part name randomization, since we rely on it
SET insert_keeper_fault_injection_probability=0;

DROP TABLE IF EXISTS t1 SYNC;

CREATE TABLE t1 (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_03356/t1', '1') ORDER BY tuple();

SYSTEM STOP PULLING REPLICATION LOG t1;

INSERT INTO t1 VALUES (1);

SYSTEM START PULLING REPLICATION LOG t1;

ALTER TABLE t1 DETACH PART 'all_0_0_0';
