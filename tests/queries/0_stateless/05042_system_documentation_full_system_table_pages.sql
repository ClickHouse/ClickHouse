-- System tables expose consistently structured reference pages assembled from
-- their metadata comment sections and live column schemas.
SELECT
    name,
    source,
    description LIKE '%## Description {#description}%' AS has_description,
    description LIKE '%## Columns {#columns}%' AS has_columns,
    description LIKE '%## Examples {#examples}%' AS has_examples
FROM system.documentation
WHERE type = 'System Table' AND name IN ('documentation', 'parts')
ORDER BY name;

-- Documentation follows the attached system tables. In particular,
-- `transactions` is normally gated by a disabled server setting and absent.
SELECT
    name,
    source,
    description LIKE '%## Columns {#columns}%' AS has_columns
FROM system.documentation
WHERE type = 'System Table' AND name IN ('asynchronous_metrics', 'transactions')
ORDER BY name;

-- Every attached table remains visible, including optional/private tables
-- which rely on an ordinary metadata comment instead of section markers.
SELECT count()
FROM system.tables
WHERE database = 'system'
    AND name NOT IN (
        SELECT name
        FROM system.documentation
        WHERE type = 'System Table');

-- Every attached table is emitted exactly once.
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

-- Page-specific sections are part of Description, while the structured
-- Columns and Examples sections retain their canonical order.
SELECT
    name,
    position(description, '## Description {#description}')
        < position(
            description,
            multiIf(
                name = 'disk_types', '## Configuration examples {#configuration-examples}',
                '## Event descriptions {#event-descriptions}'))
        AND position(
            description,
            multiIf(
                name = 'disk_types', '## Configuration examples {#configuration-examples}',
                '## Event descriptions {#event-descriptions}'))
            < position(description, '## Columns {#columns}') AS additional_section_in_description,
    position(description, '## Columns {#columns}')
        < position(description, '## Examples {#examples}') AS columns_before_examples,
    position(description, '## See also {#see-also}') = 0
        OR position(description, '## Examples {#examples}')
            < position(description, '## See also {#see-also}') AS examples_before_see_also
FROM system.documentation
WHERE type = 'System Table'
    AND name IN ('disk_types', 'events')
ORDER BY name;

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

-- MDX JSX comments, admonition wrappers, and metadata section markers are
-- authoring details and must not be exposed to consumers of the rendered Markdown.
SELECT count()
FROM system.documentation
WHERE type = 'System Table'
    AND (
        position(description, '{/*') > 0
        OR match(description, '<(/)?(Tip|Note|Info|Warning|Important|Danger)>')
        OR match(description, '(^|\\n)\\.(description|columns_notes|examples|see_also)(\\n|$)'));
