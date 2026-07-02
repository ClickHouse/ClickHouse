-- Tags: no-s3-storage, no-azure-blob-storage
-- Test that the leader_election settings are accepted and validated.

-- Basic: setting can be specified
CREATE TABLE test_leader_election_1 (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS leader_election = false;
DROP TABLE test_leader_election_1;

-- Validation: `leader_election_session_timeout` must be at least 3x `leader_election_heartbeat_interval`
CREATE TABLE test_leader_election_bad (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_heartbeat_interval = 10, leader_election_session_timeout = 5; -- { serverError BAD_ARGUMENTS }

CREATE TABLE test_leader_election_bad2 (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_heartbeat_interval = 10, leader_election_session_timeout = 10; -- { serverError BAD_ARGUMENTS }

-- Validation: `leader_election_heartbeat_interval` must be a positive number of seconds.
-- A zero value would cause an immediate-reschedule loop in the heartbeat task and make
-- `isLeader` always false (it compares elapsed time against `heartbeat_interval * 2`).
CREATE TABLE test_leader_election_zero_heartbeat (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_heartbeat_interval = 0; -- { serverError BAD_ARGUMENTS }

-- Validation: `leader_election_session_timeout` must be a positive number of seconds.
CREATE TABLE test_leader_election_zero_timeout (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true, leader_election_session_timeout = 0; -- { serverError BAD_ARGUMENTS }

-- Validation: `leader_election` requires an `S3` object storage disk with shared metadata
-- (the lease protocol relies on conditional writes that other backends do not support; `Azure`
-- is implemented but not yet test-covered, so it is rejected too). A plain local disk like the
-- default below has neither, so creation fails.
CREATE TABLE test_leader_election_local (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = true; -- { serverError BAD_ARGUMENTS }

-- Setting tiers are correct
SELECT name, tier FROM system.merge_tree_settings WHERE name LIKE 'leader_election%' ORDER BY name;
