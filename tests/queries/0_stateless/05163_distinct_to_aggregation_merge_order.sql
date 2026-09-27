SET query_plan_convert_distinct_to_aggregation = 1;
SET max_threads = 4;
SET optimize_skip_merged_partitions = 0;

DROP TABLE IF EXISTS distinct_merge_sorted;
DROP TABLE IF EXISTS distinct_merge_unsorted;

CREATE TABLE distinct_merge_sorted (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO distinct_merge_sorted VALUES (2, 20), (1, 10), (2, 30);
INSERT INTO distinct_merge_sorted VALUES (1, 10), (0, 0), (2, 20);

OPTIMIZE TABLE distinct_merge_sorted FINAL DEDUPLICATE;

-- The merged part retains sorting-key order and insertion order within each equal-key range.
SELECT k, v FROM distinct_merge_sorted ORDER BY _part_offset;

CREATE TABLE distinct_merge_unsorted (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO distinct_merge_unsorted VALUES (2, 20), (1, 10), (2, 30);
INSERT INTO distinct_merge_unsorted VALUES (1, 10), (0, 0), (2, 20);

OPTIMIZE TABLE distinct_merge_unsorted FINAL DEDUPLICATE;

-- An empty sorting key still requires a single output stream that preserves the merged row order.
SELECT k, v FROM distinct_merge_unsorted ORDER BY _part_offset;

DROP TABLE distinct_merge_sorted;
DROP TABLE distinct_merge_unsorted;
