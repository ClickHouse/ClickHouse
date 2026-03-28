-- Tags: no-s3-storage
-- Test that the leader_election setting is accepted and validated.

-- Basic: setting can be specified
CREATE TABLE test_leader_election_1 (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS leader_election = false;
DROP TABLE test_leader_election_1;

-- Validation: session_timeout must be greater than heartbeat_interval
CREATE TABLE test_leader_election_bad (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_heartbeat_interval = 10, leader_election_session_timeout = 5; -- { serverError BAD_ARGUMENTS }

CREATE TABLE test_leader_election_bad2 (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_heartbeat_interval = 10, leader_election_session_timeout = 10; -- { serverError BAD_ARGUMENTS }

-- Validation: leader_election requires object storage disk (local disk should fail at startup)
-- This creates the table but should fail when trying to startup because the disk is not object storage.
CREATE TABLE test_leader_election_local (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true; -- { serverError BAD_ARGUMENTS }
