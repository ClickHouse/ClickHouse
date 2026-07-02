SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = false;

DROP TABLE IF EXISTS t_left_prune;
CREATE TABLE t_left_prune (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_left_prune SELECT 0 FROM numbers(1000);

-- A non-zero spill threshold (the default since spilling-by-default) must NOT prevent
-- pruning of left-side columns that are absent from the join output. `count()` needs no
-- payload column, so the left key column must be dropped and no ColumnPermuteTransform
-- should appear (it would if the key column leaked into the result, hurting performance).
SELECT countSubstrings(arrayStringConcat(groupArray(explain), '\n'), 'ColumnPermuteTransform') AS permute_transforms
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM t_left_prune LEFT JOIN (SELECT number FROM numbers(10)) r ON x = number
    SETTINGS max_bytes_ratio_before_external_join = 0.5, min_joined_block_size_bytes = 0
);

-- Same expectation with an absolute byte threshold set.
SELECT countSubstrings(arrayStringConcat(groupArray(explain), '\n'), 'ColumnPermuteTransform') AS permute_transforms
FROM (
    EXPLAIN PIPELINE
    SELECT count() FROM t_left_prune LEFT JOIN (SELECT number FROM numbers(10)) r ON x = number
    SETTINGS max_bytes_before_external_join = '1G', max_bytes_ratio_before_external_join = 0
);

-- Result must be identical with and without a spill threshold.
SELECT count() FROM t_left_prune LEFT JOIN (SELECT number FROM numbers(10)) r ON x = number
    SETTINGS max_bytes_ratio_before_external_join = 0;
SELECT count() FROM t_left_prune LEFT JOIN (SELECT number FROM numbers(10)) r ON x = number
    SETTINGS max_bytes_ratio_before_external_join = 0.5;

DROP TABLE t_left_prune;
