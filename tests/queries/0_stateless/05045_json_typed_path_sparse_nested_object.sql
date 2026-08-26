DROP TABLE IF EXISTS json_sparse_nested_object;

CREATE TABLE json_sparse_nested_object
(
    j JSON(
        t Tuple(a Nullable(String), b UInt64),
        obj JSON(x Nullable(String), max_dynamic_paths = 0),
        `nested.x` Nullable(String),
        max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_nested_object
SELECT CAST(
    if(
        number = 0,
        '{"t":{"a":"rare","b":1},"obj":{"x":"rare"},"nested":{"x":"rare"}}',
        '{}'),
    'JSON(t Tuple(a Nullable(String), b UInt64), obj JSON(x Nullable(String), max_dynamic_paths = 0), `nested.x` Nullable(String), max_dynamic_paths = 0)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

SELECT
    toString(j.t),
    toString(j.obj),
    toString(tupleElement(j, 'obj')),
    toString(j.^nested)
FROM json_sparse_nested_object
ORDER BY j.t.b DESC
LIMIT 2;

ALTER TABLE json_sparse_nested_object
    MODIFY COLUMN j JSON(
        t Tuple(a Nullable(String), b UInt64),
        obj JSON(x Nullable(String), y UInt64, max_dynamic_paths = 0),
        `nested.x` Nullable(String),
        max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_nested_object'
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('obj.x', 'obj.y')
ORDER BY tupleElement(path, 1);

INSERT INTO json_sparse_nested_object SELECT * FROM json_sparse_nested_object SETTINGS optimize_on_insert = 0;
OPTIMIZE TABLE json_sparse_nested_object FINAL;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_nested_object'
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('obj.x', 'obj.y')
ORDER BY tupleElement(path, 1);

DROP TABLE json_sparse_nested_object;
