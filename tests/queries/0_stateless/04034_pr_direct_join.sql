-- Test for direct join with parallel replicas and plan serialization
-- Derived from 03742_nested_loop_join_long to reproduce specific failures

DROP TABLE IF EXISTS events;

CREATE TABLE events
(
    `Id` Nullable(UInt64),
    `Payload` String,
    `Time` DateTime,
)
ENGINE = MergeTree
ORDER BY Time;

INSERT INTO events SELECT number % 3 + 2, concat('Payload_', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(100);
INSERT INTO events SELECT NULL, concat('Payload_NULL', toString(number)), toDateTime('2024-01-01 00:00:00') + INTERVAL number MINUTES FROM numbers(10);

DROP TABLE IF EXISTS attributes;
CREATE TABLE attributes
(
    `EventId` UInt64,
    `AnotherId` Nullable(UInt64),
    `Attribute` String
)
ENGINE = MergeTree ORDER BY EventId;

INSERT INTO attributes SELECT 1 AS EventId, 1 AS AnotherId, concat('A_', toString(number)) AS Attribute FROM numbers(100_000);
INSERT INTO attributes SELECT 2 AS EventId, 2 AS AnotherId, concat('B_', toString(number)) AS Attribute FROM numbers(800_000);
INSERT INTO attributes SELECT 3 AS EventId, 3 AS AnotherId, concat('C_', toString(number)) AS Attribute FROM numbers(300_000);
INSERT INTO attributes SELECT 42 AS EventId, NULL AS AnotherId, concat('O_', toString(number)) AS Attribute FROM numbers(200_000);

-- Enable parallel replicas and plan serialization
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 2;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET serialize_query_plan = 1;

-- Original settings from the test
SET query_plan_join_swap_table = 0;
SET enable_analyzer = 1;
SET join_algorithm = 'direct';
SET min_joined_block_size_rows = 0, min_joined_block_size_bytes = 0;

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

-- Code: 48. DB::Exception: Method serialize is not implemented for JoinStepLogicalLookup. (NOT_IMPLEMENTED) on remote node
SELECT t0.Id, sum(sipHash64(t0.Payload)), count(), countIf(t1.Attribute != ''), sum(sipHash64(t1.Attribute)) AS attr_hash_sum
FROM events AS t0
JOIN attributes AS t1 ON t0.Id = t1.EventId
GROUP BY t0.Id
ORDER BY t0.Id SETTINGS parallel_replicas_local_plan=0; -- { serverError NOT_IMPLEMENTED }

DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS attributes;
