SET allow_experimental_analyzer = 1;

CREATE TABLE m (`key` UInt32) ENGINE = Merge(currentDatabase(), 'a');
CREATE TABLE b (`key` UInt32, `ID` UInt32) ENGINE = MergeTree ORDER BY key;

CREATE TABLE a1 (`day` Date, `id` UInt32) ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), a1_replicated, id);

SELECT * FROM m INNER JOIN b USING (key) WHERE ID = 1; -- { serverError UNKNOWN_TABLE, ALL_CONNECTION_TRIES_FAILED }
