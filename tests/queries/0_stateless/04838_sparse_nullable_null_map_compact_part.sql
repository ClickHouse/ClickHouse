-- Tags: no-parallel-replicas, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_sparse_nullable_null_map;

CREATE TABLE t_sparse_nullable_null_map (s Nullable(String))
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    index_granularity = 10,
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = '100G',
    ratio_of_defaults_for_sparse_serialization = 0.001,
    serialization_info_version = 'with_types',
    nullable_serialization_version = 'allow_sparse',
    write_marks_for_substreams_in_compact_parts = false;

INSERT INTO t_sparse_nullable_null_map
SELECT if(number % 2 = 0, toString(number), NULL)
FROM numbers(30);

SELECT part_type
FROM system.parts
WHERE database = currentDatabase() AND table = 't_sparse_nullable_null_map' AND active;

SELECT column, serialization_kind
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse_nullable_null_map' AND column = 's' AND active;

SELECT s.null, s.size FROM t_sparse_nullable_null_map;

DROP TABLE t_sparse_nullable_null_map;
