SET use_query_condition_cache = 0;
SET use_skip_indexes_on_data_read = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_for_disjunctions = 1;
SET allow_key_condition_coalesce_rewrite = 1;
SET optimize_extract_common_expressions = 0;

DROP TABLE IF EXISTS t_skip_index_nested_coalesce;

CREATE TABLE t_skip_index_nested_coalesce
(
    id UInt32,
    a Nullable(UInt32),
    b Nullable(UInt32),
    c Nullable(UInt32),
    d UInt32,
    INDEX a_idx a TYPE minmax GRANULARITY 1,
    INDEX b_idx b TYPE minmax GRANULARITY 1,
    INDEX c_idx c TYPE minmax GRANULARITY 1,
    INDEX d_idx d TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_skip_index_nested_coalesce
SELECT number,
       if(number % 3 = 0, NULL, toUInt32(intDiv(number, 1024))),
       if(number % 5 = 0, NULL, toUInt32(intDiv(number, 1024) + 100)),
       if(number % 7 = 0, NULL, toUInt32(intDiv(number, 1024) + 200)),
       toUInt32(intDiv(number, 1024) + 1000000)
FROM numbers(4096);

SELECT count()
FROM t_skip_index_nested_coalesce
WHERE coalesce(a, coalesce(a, b, c), b, c, d) = 2;

DROP TABLE t_skip_index_nested_coalesce;
