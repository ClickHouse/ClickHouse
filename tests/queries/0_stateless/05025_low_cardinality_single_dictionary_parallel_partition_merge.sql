DROP TABLE IF EXISTS t_lc_single_dictionary_parallel_partition_merge;

CREATE TABLE t_lc_single_dictionary_parallel_partition_merge
(
    k LowCardinality(String),
    v UInt64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    index_granularity = 1024,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;

INSERT INTO t_lc_single_dictionary_parallel_partition_merge
SELECT toString(number % 2048), number
FROM numbers(100000)
SETTINGS low_cardinality_use_single_dictionary_for_part = 1;

SELECT count(), sum(c), sum(s)
FROM
(
    SELECT k, count() AS c, sum(v) AS s
    FROM t_lc_single_dictionary_parallel_partition_merge
    GROUP BY k
)
SETTINGS
    max_threads = 4,
    group_by_two_level_threshold = 1000000000,
    group_by_two_level_threshold_bytes = 1000000000,
    merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 0,
    optimize_read_in_order = 0,
    enable_parallel_single_level_merge = 1;

DROP TABLE t_lc_single_dictionary_parallel_partition_merge;
