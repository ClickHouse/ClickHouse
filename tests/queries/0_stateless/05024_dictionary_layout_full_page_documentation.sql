-- A dictionary layout whose `Documentation.description` contains a full page is exposed verbatim through
-- `system.dictionary_layouts`, and `system.documentation` does not append synthetic structured sections.
SELECT startsWith(trimBoth(description), '# flat dictionary layout')
FROM system.dictionary_layouts
WHERE name = 'flat';

SELECT splitByChar('\n', description)[1] = '# flat dictionary layout'
FROM system.documentation
WHERE type = 'Dictionary Layout' AND name = 'flat';

SELECT position(description, '**Syntax**') = 0
FROM system.documentation
WHERE type = 'Dictionary Layout' AND name = 'flat';
