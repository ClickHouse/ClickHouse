DROP TABLE IF EXISTS json_sparse_nullable_nested_evolution;

CREATE TABLE json_sparse_nullable_nested_evolution
(
    id UInt64,
    j JSON(o Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), max_dynamic_paths = 0)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.5,
    serialization_info_version = 'with_subcolumns',
    nullable_serialization_version = 'allow_sparse';

INSERT INTO json_sparse_nullable_nested_evolution
SELECT
    number,
    CAST(if(number = 0, '{"o":{"x":"value"}}', '{"o":{}}'),
        'JSON(o Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), max_dynamic_paths = 0)')
FROM numbers(100);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'o.x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_nullable_nested_evolution'
    AND column = 'j';

ALTER TABLE json_sparse_nullable_nested_evolution
    MODIFY COLUMN j JSON(o Nullable(JSON(x Nullable(String), y UInt64, max_dynamic_paths = 0)), max_dynamic_paths = 0);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'o.x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_nullable_nested_evolution'
    AND column = 'j';

ALTER TABLE json_sparse_nullable_nested_evolution
    MODIFY COLUMN j JSON(o JSON(x Nullable(String), max_dynamic_paths = 0), max_dynamic_paths = 0);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'o.x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_nullable_nested_evolution'
    AND column = 'j';

ALTER TABLE json_sparse_nullable_nested_evolution
    MODIFY COLUMN j JSON(o Nullable(JSON(x Nullable(String), max_dynamic_paths = 0)), max_dynamic_paths = 0);

SELECT subcolumns.serializations[indexOf(subcolumns.names, 'o.x')]
FROM system.parts_columns
WHERE active
    AND database = currentDatabase()
    AND table = 'json_sparse_nullable_nested_evolution'
    AND column = 'j';

SELECT count(), countIf(j.o.x = 'value')
FROM json_sparse_nullable_nested_evolution;

DROP TABLE json_sparse_nullable_nested_evolution;
