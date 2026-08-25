-- System tables expose consistently structured reference pages assembled from
-- their embedded fields and live column schemas.
SELECT
    name,
    source,
    description LIKE '%## Description {#description}%' AS has_description,
    description LIKE '%## Columns {#columns}%' AS has_columns,
    description LIKE '%## Examples {#examples}%' AS has_examples
FROM system.documentation
WHERE type = 'System Table' AND name IN ('documentation', 'parts')
ORDER BY name;

-- Narrative associated with generated columns stays inside the Columns
-- section, ahead of the consistently named Examples section.
SELECT
    position(description, '## Columns {#columns}') < position(description, 'The `name` column') AS note_after_columns,
    position(description, 'The `name` column') < position(description, '## Examples {#examples}') AS note_before_examples
FROM system.documentation
WHERE type = 'System Table' AND name = 'databases';

-- Catalogs which are themselves exposed through system tables are rendered
-- from their registries instead of being frozen into the page template.
SELECT
    name,
    description LIKE '%### Query {#query}%' AS has_query_entry
FROM system.documentation
WHERE type = 'System Table' AND name IN ('events', 'metrics')
ORDER BY name;

-- Every placeholder owned by the system-table documentation renderer must be
-- resolved before the page is exposed. Other doubled braces can be meaningful,
-- for example in the `trace_log` SQL example which formats JSON.
SELECT count()
FROM system.documentation
WHERE type = 'System Table'
    AND (
        description LIKE '%{{PROFILE_EVENTS}}%'
        OR description LIKE '%{{CURRENT_METRICS}}%'
        OR description LIKE '%{{ASYNCHRONOUS_METRICS}}%');
