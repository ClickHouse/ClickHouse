DROP TABLE IF EXISTS json_sparse_evolution_wide;
DROP TABLE IF EXISTS json_sparse_evolution_compact;

CREATE TABLE json_sparse_evolution_wide
(
    id UInt64,
    j JSON(x Nullable(String), y String, max_dynamic_paths = 1)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

CREATE TABLE json_sparse_evolution_compact AS json_sparse_evolution_wide
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_evolution_wide
SELECT
    number,
    CAST(
        if(
            number = 0,
            '{"x":"rare","y":"dense","dynamic":1,"shared_a":"a","shared_b":"b"}',
            '{"x":null,"y":"dense","dynamic":1,"shared_a":"a","shared_b":"b"}'),
        'JSON(x Nullable(String), y String, max_dynamic_paths = 1)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

INSERT INTO json_sparse_evolution_wide
SELECT id + 100, j FROM json_sparse_evolution_wide
SETTINGS optimize_on_insert = 0;

INSERT INTO json_sparse_evolution_compact SELECT * FROM json_sparse_evolution_wide;

ALTER TABLE json_sparse_evolution_wide
    MODIFY COLUMN j JSON(x Nullable(String), z Nullable(String), max_dynamic_paths = 1);
ALTER TABLE json_sparse_evolution_compact
    MODIFY COLUMN j JSON(x Nullable(String), z Nullable(String), max_dynamic_paths = 1);

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_sparse_evolution_wide', 'json_sparse_evolution_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('x', 'z')
ORDER BY table, tupleElement(path, 1);

SELECT
    _table AS table,
    count(),
    countIf(j.x = 'rare'),
    countIf(j.z IS NULL),
    countIf(j.y = 'dense'),
    countIf(length(JSONSharedDataPaths(j)) > 0)
FROM merge('json_sparse_evolution_(wide|compact)')
GROUP BY _table
ORDER BY table;

OPTIMIZE TABLE json_sparse_evolution_wide FINAL;
OPTIMIZE TABLE json_sparse_evolution_compact FINAL;

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_sparse_evolution_wide', 'json_sparse_evolution_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('x', 'z')
ORDER BY table, tupleElement(path, 1);

DROP TABLE json_sparse_evolution_wide;
DROP TABLE json_sparse_evolution_compact;
