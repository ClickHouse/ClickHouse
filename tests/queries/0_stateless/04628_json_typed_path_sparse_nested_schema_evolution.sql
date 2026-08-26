DROP TABLE IF EXISTS json_nested_evolution_wide;
DROP TABLE IF EXISTS json_nested_evolution_compact;

CREATE TABLE json_nested_evolution_wide
(
    j JSON(t Tuple(a Nullable(String)), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

CREATE TABLE json_nested_evolution_compact AS json_nested_evolution_wide
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 1000000000,
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_nested_evolution_wide
SELECT CAST(
    if(number = 0, '{"t":{"a":"rare"}}', '{"t":{"a":null}}'),
    'JSON(t Tuple(a Nullable(String)), max_dynamic_paths = 0)')
FROM numbers(100)
SETTINGS optimize_on_insert = 0;

INSERT INTO json_nested_evolution_compact SELECT * FROM json_nested_evolution_wide;

ALTER TABLE json_nested_evolution_wide
    MODIFY COLUMN j JSON(t Tuple(a Nullable(String), b UInt64), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;
ALTER TABLE json_nested_evolution_compact
    MODIFY COLUMN j JSON(t Tuple(a Nullable(String), b UInt64), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;

SELECT _table, count(), countIf(j.t.a = 'rare'), sum(j.t.b)
FROM merge('json_nested_evolution_(wide|compact)')
GROUP BY _table
ORDER BY _table;

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_nested_evolution_wide', 'json_nested_evolution_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('t.a', 't.b')
ORDER BY table, tupleElement(path, 1);

ALTER TABLE json_nested_evolution_wide
    MODIFY COLUMN j JSON(t Tuple(c Nullable(String), b UInt64), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;
ALTER TABLE json_nested_evolution_compact
    MODIFY COLUMN j JSON(t Tuple(c Nullable(String), b UInt64), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;

SELECT _table, count(), countIf(j.t.c = 'rare'), sum(j.t.b)
FROM merge('json_nested_evolution_(wide|compact)')
GROUP BY _table
ORDER BY _table;

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_nested_evolution_wide', 'json_nested_evolution_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) IN ('t.b', 't.c')
ORDER BY table, tupleElement(path, 1);

ALTER TABLE json_nested_evolution_wide
    MODIFY COLUMN j JSON(t Tuple(c Nullable(String)), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;
ALTER TABLE json_nested_evolution_compact
    MODIFY COLUMN j JSON(t Tuple(c Nullable(String)), max_dynamic_paths = 0)
    SETTINGS mutations_sync = 2;

OPTIMIZE TABLE json_nested_evolution_wide FINAL;
OPTIMIZE TABLE json_nested_evolution_compact FINAL;

SELECT _table, count(), countIf(j.t.c = 'rare')
FROM merge('json_nested_evolution_(wide|compact)')
GROUP BY _table
ORDER BY _table;

SELECT table, tupleElement(path, 1), tupleElement(path, 2)
FROM
(
    SELECT table, arrayJoin(arrayZip(subcolumns.names, subcolumns.serializations)) AS path
    FROM system.parts_columns
    WHERE active
        AND database = currentDatabase()
        AND table IN ('json_nested_evolution_wide', 'json_nested_evolution_compact')
        AND column = 'j'
)
WHERE tupleElement(path, 1) = 't.c'
ORDER BY table, tupleElement(path, 1);

DROP TABLE json_nested_evolution_wide;
DROP TABLE json_nested_evolution_compact;
