-- Version 1 of the `uniq` state carries an additional small sample of 64-bit hashes, which fixes
-- the overflow of the estimate at cardinalities above ten billion (issue #6078). The estimate below
-- about two billion still comes from the same 32-bit sample as before, so the values here
-- match the previous versions exactly.

-- A fresh state spells its version out in the type name.
SELECT toTypeName(uniqState(1));
SELECT toTypeName(uniqState(number, number)) FROM numbers(1);

-- A fresh table gets the current version pinned at DDL time.
DROP TABLE IF EXISTS uniq_v1;
CREATE TABLE uniq_v1 (state AggregateFunction(uniq, UInt64)) ENGINE = MergeTree ORDER BY tuple();
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_v1';

INSERT INTO uniq_v1 SELECT uniqState(number) FROM numbers(1000000);

-- Reading the state locally, and through a distributed query that sends the raw column over the wire,
-- must both give the same value.
SELECT uniqMerge(state) FROM uniq_v1;
SELECT uniqMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_v1))
SETTINGS prefer_localhost_replica = 0;

-- A table with the version pinned to 0 - which is what a table created before the state had a version
-- looks like once it is attached on a server that knows about version 1 - keeps the legacy format,
-- and below the conversion threshold the two versions hold identical values.
DROP TABLE IF EXISTS uniq_legacy;
CREATE TABLE uniq_legacy (state AggregateFunction(0, uniq, UInt64)) ENGINE = MergeTree ORDER BY tuple();

-- The version is pinned to 0, and version 0 is not printed in the type name.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_legacy';

INSERT INTO uniq_legacy SELECT uniqState(number) FROM numbers(1000000);

SELECT uniqMerge(state) FROM uniq_legacy;
SELECT uniqMerge(state)
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_legacy))
SETTINGS prefer_localhost_replica = 0;

-- Choosing a version for the wire must not pin it on the table's own type, which is shared with the
-- table metadata: the column still has to be read back from its parts as version 0.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'uniq_legacy';

-- States of the two versions deduplicate against each other when merged:
-- the result has to be about 1.5 million (the union of 0..1000000 and 500000..1500000),
-- while a failure to deduplicate across the versions would give about 2 million.
INSERT INTO uniq_legacy SELECT uniqState(number) FROM numbers(500000, 1000000);
SELECT round(uniqMerge(state), -4) FROM (SELECT state FROM uniq_v1 UNION ALL SELECT state FROM uniq_legacy);

-- The same in the other order of the arguments of the merge.
SELECT round(uniqMerge(state), -4) FROM (SELECT state FROM uniq_legacy UNION ALL SELECT state FROM uniq_v1);

-- Below the sampling threshold the state is exact (up to occasional 32-bit hash collisions).
SELECT uniqMerge(state) FROM (SELECT uniqState(number) AS state FROM numbers(50000));

DROP TABLE uniq_v1;
DROP TABLE uniq_legacy;
