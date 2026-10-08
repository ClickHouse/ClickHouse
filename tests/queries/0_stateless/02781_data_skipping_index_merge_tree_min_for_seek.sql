-- Tags: no-random-merge-tree-settings, no-random-settings

DROP TABLE IF EXISTS data;

CREATE TABLE data
(
    key  Int,
    v1   DateTime,
    INDEX v1_index v1 TYPE minmax GRANULARITY 1
) ENGINE=AggregatingMergeTree()
ORDER BY key
SETTINGS index_granularity=8192;

SYSTEM STOP MERGES data;

-- generate 50% of marks that cannot be skipped with v1_index
-- this will create a gap in marks
INSERT INTO data SELECT number,     if(number/8192 % 2 == 0, now(), now() - INTERVAL 200 DAY) FROM numbers(1e6);
INSERT INTO data SELECT number+1e6, if(number/8192 % 2 == 0, now(), now() - INTERVAL 200 DAY) FROM numbers(1e6);

-- Set `parallel_replicas_index_analysis_only_on_coordinator = 0` to prevent remote replicas from skipping index analysis in Parallel Replicas.
-- Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SELECT * FROM data WHERE v1 >= now() - INTERVAL 180 DAY FORMAT Null SETTINGS max_threads=1, max_final_threads=1, force_data_skipping_indices='v1_index', merge_tree_min_rows_for_seek=0, max_rows_to_read=1999999, parallel_replicas_index_analysis_only_on_coordinator=0;
SELECT * FROM data WHERE v1 >= now() - INTERVAL 180 DAY FORMAT Null SETTINGS max_threads=1, max_final_threads=1, force_data_skipping_indices='v1_index', merge_tree_min_rows_for_seek=1, max_rows_to_read=1999999, parallel_replicas_index_analysis_only_on_coordinator=0; -- { serverError TOO_MANY_ROWS }
