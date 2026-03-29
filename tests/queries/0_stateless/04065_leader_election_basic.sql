-- Tags: no-fasttest
-- Test basic leader election functionality on S3 storage.

DROP TABLE IF EXISTS test_leader_election_s3;

-- Create a table with leader_election enabled on S3 storage.
-- The instance should become leader since it's the only writer.
CREATE TABLE test_leader_election_s3 (x UInt64, s String)
ENGINE = MergeTree ORDER BY x
SETTINGS storage_policy = 's3_cache', leader_election = true,
    leader_election_heartbeat_interval = 2, leader_election_session_timeout = 10;

-- Wait briefly for the leader election heartbeat to run and acquire leadership.
SELECT sleepEachRow(3) FROM numbers(1) FORMAT Null;

-- The single instance should be the leader and accept writes.
INSERT INTO test_leader_election_s3 SELECT number, toString(number) FROM numbers(100);

SELECT count() FROM test_leader_election_s3;

-- Verify data correctness.
SELECT sum(x) FROM test_leader_election_s3;

-- Verify mutations work (leader should allow them).
ALTER TABLE test_leader_election_s3 DELETE WHERE x >= 50 SETTINGS mutations_sync = 1;
SELECT count() FROM test_leader_election_s3;

DROP TABLE test_leader_election_s3;
