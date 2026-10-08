DROP TABLE IF EXISTS t_sparse_string_size;
CREATE TABLE t_sparse_string_size
(
    id UInt64,
    t Tuple(a Nullable(String), b Nullable(UInt64))
)
ENGINE = MergeTree ORDER BY ()
SETTINGS
    index_granularity = 10,
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    serialization_info_version = 'with_types',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO t_sparse_string_size
SELECT
    number,
    (if(number % 13 = 0, toString(number), NULL),
     if(number % 11 = 0, number, NULL))
FROM numbers(200, 30);


SELECT id, t.a.size, t FROM t_sparse_string_size WHERE id % 11 = 0;

DROP TABLE t_sparse_string_size;

