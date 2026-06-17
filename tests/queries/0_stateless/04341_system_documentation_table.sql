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
    ('Disk Type', 'local'))
ORDER BY t, name;

-- The documentation of a function is rendered as Markdown assembled from the structured parts.
SELECT description LIKE '%**Syntax**%'
FROM system.documentation WHERE type = 'Function' AND name = 'plus';

-- The full Markdown page of a table engine is embedded in the description.
SELECT description LIKE '%MergeTree%'
FROM system.documentation WHERE type = 'Table Engine' AND name = 'MergeTree';

-- Aliases are rendered as a reference to the canonical entity.
SELECT count() > 0 FROM system.documentation WHERE description LIKE 'Alias of %';
