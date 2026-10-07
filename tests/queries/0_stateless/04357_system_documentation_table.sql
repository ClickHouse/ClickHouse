-- system.documentation collects the embedded documentation of the uniform components
-- of the system (functions, table engines, data types, etc.) into a single table.
-- See https://github.com/ClickHouse/ClickHouse/issues/89377

-- Column structure, including the full set of entity kinds in the `type` enum.
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'documentation' ORDER BY position;

-- Every kind of entity is represented.
SELECT DISTINCT toString(type) AS t FROM system.documentation ORDER BY t;

-- A selection of well-known entities of each kind is present.
SELECT toString(type) AS t, name FROM system.documentation
WHERE (t, name) IN (
    ('Function', 'plus'),
    ('Aggregate Function', 'count'),
    ('Table Function', 'numbers'),
    ('Table Engine', 'MergeTree'),
    ('Database Engine', 'Atomic'),
    ('Data Type', 'String'),
    ('Dictionary Layout', 'flat'),
    ('Dictionary Source', 'file'),
    ('Aggregate Function Combinator', 'If'),
    ('Data Skipping Index', 'minmax'),
    ('Disk Type', 'local'),
    ('Setting', 'max_threads'),
    ('MergeTree Setting', 'index_granularity'),
    ('Server Setting', 'max_server_memory_usage'),
    ('Format', 'CSV'))
ORDER BY t, name;

-- The documentation of a function is rendered as Markdown assembled from the structured parts.
SELECT description LIKE '%**Syntax**%'
FROM system.documentation WHERE type = 'Function' AND name = 'plus';

-- The full Markdown page of a table engine is embedded in the description.
SELECT description LIKE '%MergeTree%'
FROM system.documentation WHERE type = 'Table Engine' AND name = 'MergeTree';

-- Aliases are rendered as a reference to the canonical entity.
SELECT count() > 0 FROM system.documentation WHERE description LIKE 'Alias of %';

-- Table function aliases are rendered as a reference to the canonical entity as well,
-- instead of duplicating the documentation of the canonical table function.
SELECT description FROM system.documentation WHERE type = 'Table Function' AND name = 'timeSeriesData';

-- It is a help surface: entities without documentation (in particular, internal functions) are not exposed,
-- so there are no rows with an empty description.
SELECT count() FROM system.documentation WHERE description = '';

-- The documentation of a setting is its (Markdown) description.
SELECT description != '' FROM system.documentation WHERE type = 'Setting' AND name = 'max_threads';

-- Obsolete settings carry only a placeholder description and are not exposed on the help surface.
SELECT count() FROM system.documentation WHERE type = 'Setting' AND description = 'Obsolete setting, does nothing.';

-- Setting aliases are rendered as a reference to the canonical setting, like the other aliased entities.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'enable_analyzer';

-- MergeTree setting aliases are rendered as a reference to the canonical setting as well.
SELECT description FROM system.documentation WHERE type = 'MergeTree Setting' AND name = 'allow_experimental_block_number_column';
