DROP TABLE IF EXISTS json_typed_path_sparse_wide;
DROP TABLE IF EXISTS json_typed_path_sparse_compact;

CREATE TABLE json_typed_path_sparse_wide
(
    j JSON(
        x Nullable(String),
        y Nullable(String),
        t Tuple(a Nullable(String), b UInt64),
        max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

CREATE TABLE json_typed_path_sparse_compact AS json_typed_path_sparse_wide
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_typed_path_sparse_wide
SELECT CAST(
    if(
        number = 0,
        '{"x":"value","y":"dense","t":{"a":"one","b":1}}',
        '{"x":null,"y":"dense","t":{"a":null,"b":1}}'),
    'JSON(x Nullable(String), y Nullable(String), t Tuple(a Nullable(String), b UInt64), max_dynamic_paths = 0)')
FROM numbers(100);

INSERT INTO json_typed_path_sparse_compact SELECT * FROM json_typed_path_sparse_wide;

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_typed_path_sparse_wide', 'json_typed_path_sparse_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('x', 'y', 't.a', 't.b')
ORDER BY table, tupleElement(path, 1);

SELECT count(), countIf(j.x = 'value'), countIf(j.y = 'dense'), countIf(j.t.a = 'one'), sum(j.t.b)
FROM json_typed_path_sparse_wide;

DETACH TABLE json_typed_path_sparse_wide;
ATTACH TABLE json_typed_path_sparse_wide;

INSERT INTO json_typed_path_sparse_wide SELECT * FROM json_typed_path_sparse_compact;
OPTIMIZE TABLE json_typed_path_sparse_wide FINAL;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_typed_path_sparse_wide'
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('x', 'y', 't.a', 't.b')
ORDER BY tupleElement(path, 1);

SELECT count(), countIf(length(toString(j)) > 0), countIf(j.x = 'value'), countIf(j.t.a = 'one'), sum(j.t.b)
FROM json_typed_path_sparse_wide;

DROP TABLE json_typed_path_sparse_wide;
DROP TABLE json_typed_path_sparse_compact;
