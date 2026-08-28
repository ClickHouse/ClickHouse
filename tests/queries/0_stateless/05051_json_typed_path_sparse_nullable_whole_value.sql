DROP TABLE IF EXISTS json_sparse_nullable_whole_value;

CREATE TABLE json_sparse_nullable_whole_value
(
    id UInt64,
    j Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_nullable_whole_value
SELECT
    number,
    CAST(multiIf(number = 0, '{"x":"value"}', number = 1, NULL, '{}'),
        'Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))')
FROM numbers(3);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_nullable_whole_value'
    AND column = 'j';

SELECT j
FROM json_sparse_nullable_whole_value
ORDER BY id
FORMAT JSONEachRow;

SELECT estimateCompressionRatio(j) > 0
FROM json_sparse_nullable_whole_value;

DROP TABLE json_sparse_nullable_whole_value;
