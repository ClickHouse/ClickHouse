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

-- The registration set, rather than the tables attached in the current
-- environment, determines which system-table pages are exposed. In particular,
-- `transactions` is normally gated by a disabled server setting.
SELECT
    name,
    source,
    description LIKE '%## Columns {#columns}%' AS has_columns
FROM system.documentation
WHERE type = 'System Table' AND name IN ('asynchronous_metrics', 'trace_log', 'transactions')
ORDER BY name;

-- Every attached table remains visible, including optional/private tables
-- which rely on their metadata comment instead of structured documentation.
SELECT count()
FROM system.tables
WHERE database = 'system'
    AND name NOT IN (
        SELECT name
        FROM system.documentation
        WHERE type = 'System Table');

-- A table which is both attached and registered is emitted exactly once.
SELECT count()
FROM
(
    SELECT name
    FROM system.documentation
    WHERE type = 'System Table'
    GROUP BY name
    HAVING count() != 1
);

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

-- MDX JSX comments and admonition wrappers are authoring details
-- and must not be exposed to consumers of the rendered Markdown.
SELECT count()
FROM system.documentation
WHERE type = 'System Table'
    AND (
        position(description, '{/*') > 0
        OR match(description, '<(/)?(Tip|Note|Info|Warning|Important|Danger)>'));
