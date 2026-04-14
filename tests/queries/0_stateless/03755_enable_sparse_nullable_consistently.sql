-- { echo ON }

DROP TABLE IF EXISTS t;

CREATE TABLE t (id UInt64, n Nullable(UInt64), s Nullable(String), t Tuple(a Nullable(String), b Nullable(UInt64))) ENGINE = MergeTree ORDER BY () SETTINGS index_granularity = 10, min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1, serialization_info_version = 'with_types', nullable_serialization_version = 'basic';

INSERT INTO t(id) VALUES (1), (2), (3);

SELECT
    column,
    substreams
FROM system.parts_columns
WHERE (database = currentDatabase()) AND (`table` = 't') AND active;

DROP TABLE t;
