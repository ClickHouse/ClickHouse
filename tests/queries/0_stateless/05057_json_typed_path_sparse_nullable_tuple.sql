DROP TABLE IF EXISTS json_typed_path_sparse_nullable_tuple;

CREATE TABLE json_typed_path_sparse_nullable_tuple
(
    id UInt64,
    t Tuple(j Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_typed_path_sparse_nullable_tuple
SELECT
    number,
    tuple(CAST(
        multiIf(number = 0, '{"x":"value"}', number = 1, NULL, '{}'),
        'Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))'))
FROM numbers(100);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'j.x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_typed_path_sparse_nullable_tuple'
    AND column = 't';

ALTER TABLE json_typed_path_sparse_nullable_tuple
    MODIFY COLUMN t Tuple(j Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), n UInt8)
    SETTINGS mutations_sync = 2;

SELECT count(), countIf(t.j.x = 'value'), countIf(isNull(t.j))
FROM json_typed_path_sparse_nullable_tuple;

DROP TABLE json_typed_path_sparse_nullable_tuple;
