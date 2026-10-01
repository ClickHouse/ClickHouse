-- Tags: zookeeper, no-shared-catalog, no-shared-merge-tree
-- Tag zookeeper: the `default_table_engine = ReplicatedMergeTree` scenario creates argument-less
-- replicated inner tables using the default replica path.
-- Tag no-shared-catalog: the test asserts `MergeTree` and `ReplicatedMergeTree` inner engines,
-- which in Cloud are replaced with `SharedMergeTree`.
-- Tag no-shared-merge-tree: the inner engines come from `default_table_engine` at runtime,
-- so `--replace-replicated-with-shared` cannot rewrite them, and they don't work on SMT disks.
--
-- The choice of the inner engines from `default_table_engine` (including the rejection of unsuitable values)
-- is covered by the unit test gtest_normalize_time_series_definition.cpp; this test checks the created inner tables.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_default_engine;

SELECT '-- default_table_engine = MergeTree generates plain inner engines';

SET default_table_engine = 'MergeTree';
CREATE TABLE ts_default_engine ENGINE = TimeSeries;

-- An inner table is named `.inner_id.<target>.<uuid>`, so `splitByChar('.', name)[3]` is the target kind.
SELECT splitByChar('.', name)[3] AS target, engine FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\_id.%' ORDER BY target;

DROP TABLE ts_default_engine;

SELECT '-- default_table_engine = ReplicatedMergeTree generates replicated inner engines';

SET default_table_engine = 'ReplicatedMergeTree';
CREATE TABLE ts_default_engine ENGINE = TimeSeries;

SELECT splitByChar('.', name)[3] AS target, engine FROM system.tables
WHERE database = currentDatabase() AND name LIKE '.inner\_id.%' ORDER BY target;

DROP TABLE ts_default_engine SYNC;
