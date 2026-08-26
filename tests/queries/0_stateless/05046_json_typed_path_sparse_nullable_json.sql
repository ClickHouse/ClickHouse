DROP TABLE IF EXISTS json_sparse_nullable_nested;
DROP TABLE IF EXISTS json_sparse_nullable_top_level;

CREATE TABLE json_sparse_nullable_nested
(
    j JSON(o Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_nullable_nested
SELECT CAST(
    multiIf(number = 0, '{"o":{"x":"rare"}}', number = 1, '{"o":null}', '{}'),
    'JSON(o Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), max_dynamic_paths = 0)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_nullable_nested'
        AND column = 'j'
)
WHERE tupleElement(path, 1) = 'o.x';

SELECT count(), countIf(j.o.x = 'rare'), countIf(j.o IS NULL)
FROM json_sparse_nullable_nested;

CREATE TABLE json_sparse_nullable_top_level
(
    j Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_nullable_top_level
SELECT CAST(
    multiIf(number = 0, '{"x":"rare"}', number = 1, NULL, '{}'),
    'Nullable(JSON(x Nullable(String), max_dynamic_paths = 0))')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

SELECT tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table = 'json_sparse_nullable_top_level'
        AND column = 'j'
)
WHERE tupleElement(path, 1) = 'x';

SELECT count(), countIf(j.x = 'rare'), countIf(j IS NULL)
FROM json_sparse_nullable_top_level;

DROP TABLE json_sparse_nullable_nested;
DROP TABLE json_sparse_nullable_top_level;
