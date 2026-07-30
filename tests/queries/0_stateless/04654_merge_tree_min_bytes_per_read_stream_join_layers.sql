-- Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage

DROP TABLE IF EXISTS layer_left;
DROP TABLE IF EXISTS layer_right;

CREATE TABLE layer_left (k UInt64, w UInt8) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
CREATE TABLE layer_right (k UInt64, w UInt8) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO layer_left SELECT number, number % 251 FROM numbers(2000000);
INSERT INTO layer_right SELECT number, number % 251 FROM numbers(2000000);

WITH
    (SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w)
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS enable_analyzer = 1, query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 1,
            enable_join_runtime_filters = 0, max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0,
            merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1,
            merge_tree_min_bytes_per_read_stream = 65536)) AS capped,
    (SELECT max(toUInt32OrZero(extract(explain, 'MergeTreeSelect.*× (\\d+)')))
     FROM (EXPLAIN PIPELINE
        SELECT sum(l.w + r.w)
        FROM layer_left AS l INNER JOIN layer_right AS r USING k
        SETTINGS enable_analyzer = 1, query_plan_join_swap_table = 0, query_plan_join_shard_by_pk_ranges = 1,
            enable_join_runtime_filters = 0, max_threads = 64, merge_tree_min_rows_for_concurrent_read = 0,
            merge_tree_min_bytes_for_concurrent_read = 0, merge_tree_min_read_task_size = 1,
            merge_tree_min_bytes_per_read_stream = 0)) AS uncapped
SELECT capped < uncapped;

DROP TABLE layer_left;
DROP TABLE layer_right;
