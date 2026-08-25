-- System tables expose their complete embedded reference pages, not only their
-- generated column lists.
SELECT
    name,
    source,
    description LIKE '%## Description {#description}%' AS has_description,
    description LIKE '%## Columns {#columns}%' AS has_columns,
    description LIKE '%## Example {#example}%' AS has_example
FROM system.documentation
WHERE type = 'System Table' AND name IN ('documentation', 'parts')
ORDER BY name;

-- Catalogs which are themselves exposed through system tables are rendered
-- from their registries instead of being frozen into the page template.
SELECT
    name,
    description LIKE '%### Query {#query}%' AS has_query_entry
FROM system.documentation
WHERE type = 'System Table' AND name IN ('events', 'metrics')
ORDER BY name;

-- Every template placeholder must be resolved before the page is exposed.
SELECT count()
FROM system.documentation
WHERE type = 'System Table'
    AND source = 'src/Storages/System/SystemTableDocumentation.inc'
    AND description LIKE '%{{%}}%';
