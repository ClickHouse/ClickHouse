-- The `-Tuple` combinator is a pass-through combinator: it keeps one nested state per tuple element
-- inside its own state and forwards the version to every nested state in `serialize` / `deserialize`.
-- Its state type therefore has to spell the version out, exactly like the other pass-through
-- combinators. Otherwise a fresh state column is created at the default version and every local
-- serialization round trip of the column (`groupArray` over the states, sorting, views) writes the
-- nested `uniq` states in the legacy version 0 layout and drops the 64-bit sample.

SELECT toTypeName(uniqTupleState((1, 2)));
SELECT toTypeName(uniqTupleState((toNullable(1), 2)));

-- A `-Tuple` of an unversioned function stays unversioned.
SELECT toTypeName(sumTupleState((1, 2)));
SELECT toTypeName(avgTupleState((1, 2)));

-- A `groupArray` round trip of fresh states serializes each state into an arena and back;
-- the version must survive it, so the merged estimate must match the direct one.
SELECT uniqTupleMerge(arrayJoin(states))
FROM (SELECT groupArray(state) AS states FROM (SELECT uniqTupleState((number % 3, number)) AS state FROM numbers(100000)));
SELECT uniqTupleMerge(state) FROM (SELECT uniqTupleState((number % 3, number)) AS state FROM numbers(100000));

DROP TABLE IF EXISTS uniq_tuple_v1;
CREATE TABLE uniq_tuple_v1 (state AggregateFunction(uniqTuple, Tuple(UInt64, UInt64))) ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_tuple_v1';

INSERT INTO uniq_tuple_v1 SELECT uniqTupleState((toUInt64(number % 3), number)) FROM numbers(100000);

-- Reading the state locally, and through a distributed query that sends the raw column over the wire,
-- must both round-trip the per-element states.
SELECT uniqTupleMerge(state) FROM uniq_tuple_v1;
SELECT uniqTupleMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_tuple_v1))
SETTINGS prefer_localhost_replica = 0;

DROP TABLE uniq_tuple_v1;
