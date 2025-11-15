CREATE TABLE data_02200 (
    key Int,
    value Int,
    INDEX idx value TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key
PARTITION BY key;

set use_query_condition_cache = false;
set use_skip_indexes_on_data_read = false;

INSERT INTO data_02200 SELECT number, number FROM numbers(10);

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

-- { echoOn }
SELECT * FROM data_02200 WHERE value = 1 SETTINGS use_skip_indexes=1, max_rows_to_read=1;
SELECT * FROM data_02200 WHERE value = 1 SETTINGS use_skip_indexes=0, max_rows_to_read=1; -- { serverError TOO_MANY_ROWS }
