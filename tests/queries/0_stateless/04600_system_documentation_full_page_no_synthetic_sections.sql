-- A full-page description (the entire website reference page embedded in the code) is published in
-- `system.documentation` as-is: the sections composed from the structured metadata fields (`syntax`,
-- `examples`, `related`, `introduced_in`) must not be appended after it, because the page body already
-- covers that material. See https://github.com/ClickHouse/ClickHouse/pull/110195

-- `Enum` keeps structured `syntax`/`related` metadata alongside a full-page description;
-- the rendered documentation must not duplicate them as synthetic sections.
SELECT 'enum_no_synthetic_sections',
    countSubstrings(description, '**Syntax**')
    + countSubstrings(description, '**Related:**')
    + countSubstrings(description, '**Introduced in:**')
FROM system.documentation WHERE type = 'Data Type' AND name = 'Enum';

-- The full page body itself is preserved (a section header with its explicit anchor).
SELECT 'enum_page_body_kept', description LIKE '%## ADD ENUM VALUES {#add-enum-values}%'
FROM system.documentation WHERE type = 'Data Type' AND name = 'Enum';

-- A short summary with structured fields still gets the composed sections
-- (`like` has an incidental header without an anchor in its summary, which must not count as a full page).
SELECT 'like_composed_sections_kept', description LIKE '%**Syntax**%' AND description LIKE '%**Examples**%'
FROM system.documentation WHERE type = 'Function' AND name = 'like';

-- No entity mixes a full page (an anchored section header) with appended synthetic sections.
SELECT 'no_full_page_with_synthetic_tail', count()
FROM system.documentation
WHERE match(description, '(^|\n)#{1,6} [^\n]*\\{#[^}]+\\}')
    AND (description LIKE '%**Related:**%' OR description LIKE '%**Introduced in:**%');
