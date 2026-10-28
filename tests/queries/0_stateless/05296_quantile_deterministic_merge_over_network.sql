-- A `-Merge` of stored `quantileDeterministic` states has to give the same result when the partial
-- merge runs on another server: the state crosses the network and has to keep the reservoir's skip
-- degree, or the result depends on how the rows were split between the servers.
-- https://github.com/ClickHouse/ClickHouse/issues/122935

DROP TABLE IF EXISTS qd_raw;
DROP TABLE IF EXISTS qd_states;
DROP TABLE IF EXISTS qd_merge_states;
DROP TABLE IF EXISTS sm_states;

CREATE TABLE qd_raw (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO qd_raw SELECT number FROM numbers(1000000);

-- The split of the issue (pieces of 1, 999, 9000 and 990000 rows), stored for g = 0 and g = 1.
CREATE TABLE qd_states (g UInt8, part UInt8, state AggregateFunction(medianDeterministic, UInt64, UInt64))
ENGINE = MergeTree ORDER BY part;
INSERT INTO qd_states SELECT g, 0, medianDeterministicState(number, number) FROM numbers(1) ARRAY JOIN [0, 1] AS g GROUP BY g;
INSERT INTO qd_states SELECT g, 1, medianDeterministicState(number, number) FROM numbers(1, 999) ARRAY JOIN [0, 1] AS g GROUP BY g;
INSERT INTO qd_states SELECT g, 2, medianDeterministicState(number, number) FROM numbers(1000, 9000) ARRAY JOIN [0, 1] AS g GROUP BY g;
INSERT INTO qd_states SELECT g, 3, medianDeterministicState(number, number) FROM numbers(10000, 990000) ARRAY JOIN [0, 1] AS g GROUP BY g;

-- Arm A. The local merge, then the query of the issue: the largest state is on the other shard.
SELECT medianDeterministicMerge(state) FROM qd_states WHERE g = 0;
SELECT medianDeterministicMerge(state)
FROM remote('127.0.0.{1,2}', currentDatabase(), qd_states)
WHERE g = 0 AND (part < 3) = (shardNum() = 1);

-- Arm B. With GROUP BY, a shard sends several states in one block, which is marshalled in parallel.
SELECT g, medianDeterministicMerge(state)
FROM remote('127.0.0.{1,2}', currentDatabase(), qd_states)
WHERE (part < 3) = (shardNum() = 1)
GROUP BY g ORDER BY g
SETTINGS enable_parallel_blocks_marshalling = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- Arm C. The same for the plain function, compared with the local result.
SELECT n % 2 AS k, medianDeterministic(n, n) FROM qd_raw GROUP BY k ORDER BY k;
SELECT n % 2 AS k, medianDeterministic(n, n)
FROM remote('127.0.0.{1,2}', currentDatabase(), qd_raw)
WHERE (n < 990000) = (shardNum() = 1)
GROUP BY k ORDER BY k
SETTINGS enable_parallel_blocks_marshalling = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- Arm D. A column declared with the `-Merge` state type gets the current state version at CREATE, as
-- the column of the function itself does, so its states keep the skip degree in storage as well.
CREATE TABLE qd_merge_states
(
    g UInt8,
    part UInt8,
    state AggregateFunction(medianDeterministicMerge, AggregateFunction(medianDeterministic, UInt64, UInt64))
)
ENGINE = MergeTree ORDER BY part;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'qd_merge_states' AND name = 'state';
INSERT INTO qd_merge_states SELECT g, part, state FROM qd_states;
-- `-Merge` does not nest, so the stored states are read back as states of `medianDeterministic`.
SELECT medianDeterministicMerge(CAST(state, 'AggregateFunction(medianDeterministic, UInt64, UInt64)')) FROM qd_merge_states WHERE g = 0;

-- Arm E. The same for a -Merge of sumMap states: a shard must send its sums without cutting them to the UInt8 value type.
CREATE TABLE sm_states
(
    g UInt8,
    part UInt8,
    s AggregateFunction(sumMap, Array(UInt8), Array(UInt8)),
    m AggregateFunction(maxMap, Array(UInt8), Array(UInt8))
)
ENGINE = MergeTree ORDER BY part;
INSERT INTO sm_states
SELECT g, part, sumMapState([toUInt8(1)], [toUInt8(200)]), maxMapState([toUInt8(1)], [toUInt8(200)])
FROM (SELECT arrayJoin([0, 1]) AS g, arrayJoin([0, 1, 2, 3]) AS part)
GROUP BY g, part;
SELECT sumMapMerge(s) FROM remote('127.0.0.{1,2}', currentDatabase(), sm_states)
WHERE g = 0 AND (part = 0) = (shardNum() = 1);
SELECT g, sumMapMerge(s) FROM remote('127.0.0.{1,2}', currentDatabase(), sm_states)
WHERE (part = 0) = (shardNum() = 1)
GROUP BY g ORDER BY g
SETTINGS enable_parallel_blocks_marshalling = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
SELECT g, maxMapMerge(m) FROM remote('127.0.0.{1,2}', currentDatabase(), sm_states)
WHERE (part = 0) = (shardNum() = 1)
GROUP BY g ORDER BY g
SETTINGS enable_parallel_blocks_marshalling = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

DROP TABLE sm_states;
DROP TABLE qd_merge_states;
DROP TABLE qd_states;
DROP TABLE qd_raw;
