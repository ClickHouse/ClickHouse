-- Table engines expose embedded documentation via system.table_engines.

-- Representative engines must have a non-empty description and syntax.
SELECT name, length(description) > 0 AS has_description, length(syntax) > 0 AS has_syntax
FROM system.table_engines
WHERE name IN ('MergeTree', 'ReplicatedMergeTree', 'Memory', 'Distributed', 'Merge', 'Null')
ORDER BY name;

-- The MergeTree documentation is migrated in full, so it contains its section headers.
SELECT
    position(description, '## Projections') > 0 AS has_projections_section,
    position(description, '## Data storage') > 0 AS has_data_storage_section
FROM system.table_engines
WHERE name = 'MergeTree';

-- Related engines are exposed as an array.
SELECT related
FROM system.table_engines
WHERE name = 'MergeTree';
