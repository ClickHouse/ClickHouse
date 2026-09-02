CREATE TABLE 03811_sparse_column_aggregation_with_sum(key UInt128, val UInt16) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO 03811_sparse_column_aggregation_with_sum
    SELECT number, number % 10000 = 0 FROM numbers(100000)
    SETTINGS min_insert_block_size_rows = 1000,
             max_block_size =1000,
             max_threads = 2;

SELECT key, sum(val) AS c
FROM 03811_sparse_column_aggregation_with_sum
GROUP BY key
ORDER BY c DESC
LIMIT 100
FORMAT Null
SETTINGS group_by_overflow_mode = 'any',
         max_rows_to_group_by = 100,
         group_by_two_level_threshold_bytes = 1,
         group_by_two_level_threshold = 1,
         max_threads = 2;

DROP TABLE 03811_sparse_column_aggregation_with_sum;
