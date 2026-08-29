-- The `-ForEach` and `-Map` combinators forward `isVersioned` to the nested function, so a table
-- column declared as `AggregateFunction(uniqForEach, ...)` / `AggregateFunction(uniqMap, ...)` gets
-- version 1 pinned at DDL time just like plain `uniq` does. The combinators must then thread the
-- version through `serialize` and `deserialize` symmetrically: dropping it on one side
-- desynchronizes the written payload from the later read (the nested `uniq` falls back to its
-- default version 0 on write, but the read expects the version 1 flags byte).

-- A fresh state spells the nested function's current state version out in the type name, just like
-- plain `uniqState` does. Otherwise local serialization round trips of the column (`groupArray`
-- over the states, sorting, views) would fall back to the legacy version 0 and lose the 64-bit sample.
SELECT toTypeName(uniqForEachState([1, 2]));
SELECT toTypeName(uniqMapState(map(1, 2)));

-- The same holds for every pass-through wrapper of a versioned function: the `Nullable` adaptor,
-- `-If`, `-Array`, `-Distinct`, `-OrNull` / `-OrDefault`, `-ArgMin` / `-ArgMax` and `-Resample`.
-- A wrapper of an unversioned function stays unversioned.
SELECT toTypeName(uniqState(toNullable(1)));
SELECT toTypeName(uniqIfState(1, 1));
SELECT toTypeName(uniqArrayState([1]));
SELECT toTypeName(uniqDistinctState(1));
SELECT toTypeName(uniqOrNullState(1));
SELECT toTypeName(uniqOrDefaultState(1));
SELECT toTypeName(uniqArgMinState(1, 1));
SELECT toTypeName(uniqResampleState(1, 1, 1)(1, 1));
SELECT toTypeName(uniqExactState(toNullable(1)));
SELECT toTypeName(sumMapState(map(1, 2)));

-- A `groupArray` round trip of fresh states serializes each state into an arena and back;
-- the version must survive it, so the merged estimate must match the direct one.
SELECT uniqForEachMerge(arrayJoin(states))
FROM (SELECT groupArray(state) AS states FROM (SELECT uniqForEachState([number % 3, number]) AS state FROM numbers(100000)));
SELECT uniqMapMerge(arrayJoin(states))
FROM (SELECT groupArray(state) AS states FROM (SELECT uniqMapState(map(number % 2, number)) AS state FROM numbers(100000)));

DROP TABLE IF EXISTS uniq_foreach_v1;
CREATE TABLE uniq_foreach_v1 (state AggregateFunction(uniqForEach, Array(UInt64))) ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_foreach_v1';

INSERT INTO uniq_foreach_v1 SELECT uniqForEachState([number % 3, number]) FROM numbers(100000);

-- Reading the state locally, and through a distributed query that sends the raw column over the wire,
-- must both round-trip the per-element states.
SELECT uniqForEachMerge(state) FROM uniq_foreach_v1;
SELECT uniqForEachMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_foreach_v1))
SETTINGS prefer_localhost_replica = 0;

DROP TABLE uniq_foreach_v1;

DROP TABLE IF EXISTS uniq_map_v1;
CREATE TABLE uniq_map_v1 (state AggregateFunction(uniqMap, Map(UInt8, UInt64))) ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_map_v1';

INSERT INTO uniq_map_v1 SELECT uniqMapState(map(number % 2, number)) FROM numbers(100000);

SELECT uniqMapMerge(state) FROM uniq_map_v1;
SELECT uniqMapMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_map_v1))
SETTINGS prefer_localhost_replica = 0;

DROP TABLE uniq_map_v1;

-- A version pinned to 0 - what a table created before `uniq` had a version looks like once it is
-- attached on a server that knows about version 1 - keeps the legacy format under the combinators too.
DROP TABLE IF EXISTS uniq_foreach_legacy;
CREATE TABLE uniq_foreach_legacy (state AggregateFunction(0, uniqForEach, Array(UInt64))) ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_foreach_legacy';

INSERT INTO uniq_foreach_legacy SELECT uniqForEachState([number % 3, number]) FROM numbers(100000);
SELECT uniqForEachMerge(state) FROM uniq_foreach_legacy;

DROP TABLE uniq_foreach_legacy;
