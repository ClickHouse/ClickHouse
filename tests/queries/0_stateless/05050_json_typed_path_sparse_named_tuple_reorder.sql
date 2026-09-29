DROP TABLE IF EXISTS json_sparse_named_tuple_reorder;

CREATE TABLE json_sparse_named_tuple_reorder
(
    j JSON(t Tuple(a Nullable(String), b Nullable(String)), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_named_tuple_reorder
SELECT CAST(
    if(number = 0, '{"t":{"a":"rare","b":"dense"}}', '{"t":{"a":null,"b":"dense"}}'),
    'JSON(t Tuple(a Nullable(String), b Nullable(String)), max_dynamic_paths = 0)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

ALTER TABLE json_sparse_named_tuple_reorder
    MODIFY COLUMN j JSON(t Tuple(b Nullable(String), a Nullable(String)), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_named_tuple_reorder'
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('t.a', 't.b')
ORDER BY tupleElement(path, 1);

DROP TABLE json_sparse_named_tuple_reorder;
