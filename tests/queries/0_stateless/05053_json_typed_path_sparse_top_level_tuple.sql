DROP TABLE IF EXISTS json_typed_path_sparse_top_level_tuple;

CREATE TABLE json_typed_path_sparse_top_level_tuple
(
    id UInt64,
    t Tuple(j JSON(x Nullable(String), max_dynamic_paths = 0))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_typed_path_sparse_top_level_tuple
SELECT
    number,
    tuple(CAST(
        if(number = 0, '{"x":"value"}', '{"x":null}'),
        'JSON(x Nullable(String), max_dynamic_paths = 0)'))
FROM numbers(3);

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_typed_path_sparse_top_level_tuple'
        AND column = 't'
)
WHERE tupleElement(path, 1) = 'j.x';

ALTER TABLE json_typed_path_sparse_top_level_tuple
    MODIFY COLUMN t Tuple(j JSON(x Nullable(String), max_dynamic_paths = 0), n UInt64)
    SETTINGS mutations_sync = 2;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_typed_path_sparse_top_level_tuple'
        AND column = 't'
)
WHERE tupleElement(path, 1) = 'j.x';

SELECT t
FROM json_typed_path_sparse_top_level_tuple
ORDER BY id
FORMAT JSONEachRow;

DROP TABLE json_typed_path_sparse_top_level_tuple;
