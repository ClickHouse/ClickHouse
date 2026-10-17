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

-- The three settings are immutable after creation (`MergeTreeSettings::isReadonlySetting`):
-- an ordinary `MergeTree` table must not be able to switch the lease protocol on (or change its
-- timings) with `ALTER TABLE ... MODIFY/RESET SETTING`. Turning `leader_election` on for a table
-- whose data is already there — or off while other nodes share the same data — would run the
-- storage under a different write contract than the one it was created with.
CREATE TABLE test_leader_election_alter (x UInt64) ENGINE = MergeTree ORDER BY x;

ALTER TABLE test_leader_election_alter MODIFY SETTING leader_election = 1; -- { serverError READONLY_SETTING }
ALTER TABLE test_leader_election_alter MODIFY SETTING leader_election_heartbeat_interval = 3; -- { serverError READONLY_SETTING }
ALTER TABLE test_leader_election_alter MODIFY SETTING leader_election_session_timeout = 30; -- { serverError READONLY_SETTING }

-- Same for `RESET SETTING`, which needs a table that has the settings in its definition
-- (resetting a setting that was never set is a no-op that never reaches the readonly check).
CREATE TABLE test_leader_election_reset (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS leader_election = false, leader_election_heartbeat_interval = 3, leader_election_session_timeout = 30;

ALTER TABLE test_leader_election_reset RESET SETTING leader_election; -- { serverError READONLY_SETTING }
ALTER TABLE test_leader_election_reset RESET SETTING leader_election_heartbeat_interval; -- { serverError READONLY_SETTING }
ALTER TABLE test_leader_election_reset RESET SETTING leader_election_session_timeout; -- { serverError READONLY_SETTING }

-- The settings are still exactly the ones the table was created with.
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 'test_leader_election_reset'
    AND engine_full LIKE '%leader_election = false%'
    AND engine_full LIKE '%leader_election_heartbeat_interval = 3%'
    AND engine_full LIKE '%leader_election_session_timeout = 30%';

DROP TABLE test_leader_election_reset;

-- None of the rejected commands changed the table definition, and the table is still usable
-- (it is a plain local `MergeTree`, which `leader_election` would have rejected at creation).
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 'test_leader_election_alter' AND engine_full LIKE '%leader_election%';

INSERT INTO test_leader_election_alter VALUES (1);
SELECT count() FROM test_leader_election_alter;

DROP TABLE test_leader_election_alter;

-- Setting tiers are correct
SELECT name, tier FROM system.merge_tree_settings WHERE name LIKE 'leader_election%' ORDER BY name;
