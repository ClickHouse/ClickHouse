-- Regression test: reading `.size` subcolumn of a sparse Nullable(String) inside
-- a Tuple together with the full Tuple via PREWHERE used to cause a LOGICAL_ERROR
-- because the cached accumulated ColumnString broke ColumnSparse invariants.

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
FROM numbers(1000);

OPTIMIZE TABLE t_sparse_string_size FINAL;

-- The bug manifested when reading t.a.size and t in the same readRows call
-- across multiple granules with PREWHERE.
SELECT t.a.size, id FROM t_sparse_string_size PREWHERE id % 11 = 0 WHERE toString(t) != '' ORDER BY id LIMIT 3;

DROP TABLE t_sparse_string_size;
