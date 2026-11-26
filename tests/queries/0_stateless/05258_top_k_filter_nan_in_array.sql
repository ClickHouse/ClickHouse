-- `__topKFilter` must keep the rows that rank before the top-K threshold. With `NULLS FIRST` a NaN ranks
-- first, also as an element of an `Array` or `Tuple` key, which is compared lexicographically. The plain
-- comparison functions place such a NaN last, so the filter has to use the column comparison for them.
-- Several blocks let the threshold be published before all NaN rows are read.

SET max_threads = 1;
SET max_block_size = 1000;
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_top_k_nan_array;

CREATE TABLE t_top_k_nan_array
(
    arr Array(Float64),
    tup Tuple(UInt8, Array(Float64)),
    val UInt64
)
ENGINE = MergeTree ORDER BY val SETTINGS index_granularity = 1000;

INSERT INTO t_top_k_nan_array
SELECT if(number % 500 = 0, [nan], [toFloat64(number)]), (1, if(number % 500 = 0, [nan], [toFloat64(number)])), number
FROM numbers(50000);

SELECT 'Array ORDER BY ASC NULLS FIRST';
SELECT countIf(isNaN(arr[1])) FROM (SELECT arr FROM t_top_k_nan_array ORDER BY arr ASC NULLS FIRST LIMIT 150);
SELECT 'Array ORDER BY DESC NULLS FIRST';
SELECT countIf(isNaN(arr[1])) FROM (SELECT arr FROM t_top_k_nan_array ORDER BY arr DESC NULLS FIRST LIMIT 150);
SELECT 'Tuple with Array ORDER BY ASC NULLS FIRST';
SELECT countIf(isNaN(tup.2[1])) FROM (SELECT tup FROM t_top_k_nan_array ORDER BY tup ASC NULLS FIRST LIMIT 150);
SELECT 'Array GROUP BY ASC NULLS FIRST';
SELECT arr, count() FROM t_top_k_nan_array GROUP BY arr ORDER BY arr ASC NULLS FIRST LIMIT 2;
SELECT 'Array GROUP BY DESC NULLS FIRST';
SELECT arr, count() FROM t_top_k_nan_array GROUP BY arr ORDER BY arr DESC NULLS FIRST LIMIT 2;

DROP TABLE t_top_k_nan_array;
