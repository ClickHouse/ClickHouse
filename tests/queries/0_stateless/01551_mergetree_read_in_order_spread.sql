-- Tags: no-object-storage, no-random-merge-tree-settings

DROP TABLE IF EXISTS data_01551;

CREATE TABLE data_01551
(
    key        UInt32
) engine=AggregatingMergeTree()
PARTITION BY key%2
ORDER BY (key, key/2)
SETTINGS index_granularity=10, index_granularity_bytes='10Mi';

SET optimize_trivial_insert_select = 1;
INSERT INTO data_01551 SELECT number FROM numbers(100000);
SET max_threads=3;
SET merge_tree_min_rows_for_concurrent_read=10000;
SET optimize_aggregation_in_order=1;
SET read_in_order_two_level_merge_threshold=1;
EXPLAIN PIPELINE SELECT key FROM data_01551 GROUP BY key, key/2;

DROP TABLE data_01551;
