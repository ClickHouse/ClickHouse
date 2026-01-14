SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS minmax_compact;

CREATE TABLE minmax_compact
(
    u64 UInt64,
    i64 Int64,
    i32 Int32
) ENGINE = MergeTree()
PARTITION BY i32
ORDER BY u64
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi', min_rows_for_wide_part = 1000000;

INSERT INTO minmax_compact VALUES (0, 2, 1), (1, 1, 1), (2, 1, 1), (3, 1, 1), (4, 1, 1), (5, 2, 1), (6, 1, 2), (7, 1, 2), (8, 1, 2), (9, 1, 2);

SET mutations_sync = 1;
ALTER TABLE minmax_compact ADD INDEX idx (i64, u64 * i64) TYPE minmax GRANULARITY 1;

ALTER TABLE minmax_compact MATERIALIZE INDEX idx IN PARTITION 1;
set max_rows_to_read = 8;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER TABLE minmax_compact MATERIALIZE INDEX idx IN PARTITION 2;
set max_rows_to_read = 6;
SELECT count() FROM minmax_compact WHERE i64 = 2;

ALTER TABLE minmax_compact CLEAR INDEX idx IN PARTITION 1;
ALTER TABLE minmax_compact CLEAR INDEX idx IN PARTITION 2;

SELECT count() FROM minmax_compact WHERE i64 = 2; -- { serverError TOO_MANY_ROWS }

set max_rows_to_read = 10;
SELECT count() FROM minmax_compact WHERE i64 = 2;

DROP TABLE minmax_compact;
