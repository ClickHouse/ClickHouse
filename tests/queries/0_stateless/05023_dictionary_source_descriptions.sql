-- Every registered dictionary source provides documentation for the generated reference pages.
SELECT count() = countIf(notEmpty(trimBoth(description)))
FROM system.dictionary_sources;

-- An H1 identifies the description as a complete page, so `system.documentation` preserves it without
-- appending synthetic sections from the structured documentation fields.
SELECT splitByChar('\n', description)[1] = '# Local File dictionary source'
FROM system.documentation
WHERE type = 'Dictionary Source' AND name = 'file';

SELECT position(description, '**Syntax**') = 0 AND position(description, '**Related:**') = 0
FROM system.documentation
WHERE type = 'Dictionary Source' AND name = 'file';

-- Full-page detection also scans past MDX imports before the H1.
SELECT
    startsWith(description, 'import ')
    AND position(description, '\n# YTsaurus dictionary source') > 0
    AND position(description, '**Syntax**') = 0
FROM system.documentation
WHERE type = 'Dictionary Source' AND name = 'ytsaurus';
