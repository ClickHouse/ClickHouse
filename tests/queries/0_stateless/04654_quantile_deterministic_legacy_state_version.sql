-- Tags: no-replicated-database
-- Tag no-replicated-database: a `Replicated` database re-parses the column type from the formatted
-- `CREATE`, and version 0 is not printed in the type name, so the pin this test is about does not
-- survive the round trip through the DDL log.

-- A `quantileDeterministic` state column whose version is pinned to 0 - which is what a table created
-- before the state had a version looks like once it is attached on a server that knows about version 1 -
-- must survive a round trip through `Native` transport. Version 0 is not printed in the type name, so
-- the version that goes on the wire has to be derived from the negotiated revision on both ends. Taking
-- it from the local type on the sending side made the receiver read a version 1 state out of a version 0
-- payload, which lost sync with the rest of the stream (`Unknown codec family code: 0`).

DROP TABLE IF EXISTS legacy_quantile_deterministic;

CREATE TABLE legacy_quantile_deterministic (state AggregateFunction(0, medianDeterministic, UInt64, UInt64))
ENGINE = MergeTree ORDER BY tuple();

-- Enough rows for the reservoir to be thinned out, so that version 1 has a non-zero skip degree to write.
INSERT INTO legacy_quantile_deterministic SELECT medianDeterministicState(number, number) FROM numbers(1000000);

-- The version is pinned to 0, and version 0 is not printed in the type name.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'legacy_quantile_deterministic';

-- Reading the state locally, and through a distributed query that sends the raw column over the wire,
-- must both give the same value.
SELECT medianDeterministicMerge(state) FROM legacy_quantile_deterministic;

SELECT medianDeterministicMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), legacy_quantile_deterministic))
SETTINGS prefer_localhost_replica = 0;

-- Choosing a version for the wire must not pin it on the table's own type, which is shared with the
-- table metadata: the column still has to be read back from its parts as version 0.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'legacy_quantile_deterministic';

DROP TABLE legacy_quantile_deterministic;
