-- Tags: zookeeper

-- Regression test for missing Coordination component in PartMovesBetweenShardsOrchestrator.
-- When enforce_keeper_component_tracking is enabled, querying system.part_moves_between_shards
-- would crash with "Current component is empty" because the orchestrator methods did not set
-- the coordination component before making ZooKeeper requests.

DROP TABLE IF EXISTS test_part_moves_keeper_component;

CREATE TABLE test_part_moves_keeper_component (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_part_moves_keeper_component', 'r1')
ORDER BY id;

INSERT INTO test_part_moves_keeper_component VALUES (1);

-- This query triggers PartMovesBetweenShardsOrchestrator::getEntries() -> syncStateFromZK()
-- which makes ZooKeeper calls. Without the fix, this crashes with LOGICAL_ERROR.
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase();

-- Also test with a subquery filter (matching the stress test pattern from 02841_not_ready_set_bug.sh)
SELECT count() FROM system.part_moves_between_shards WHERE database IN (SELECT currentDatabase());

DROP TABLE test_part_moves_keeper_component;
