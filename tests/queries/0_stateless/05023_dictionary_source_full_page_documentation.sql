-- A dictionary source whose `Documentation.description` contains a full page is exposed verbatim through
-- `system.dictionary_sources`, and `system.documentation` does not append synthetic structured sections.
SELECT startsWith(trimBoth(description), '# Local File dictionary source')
FROM system.dictionary_sources
WHERE name = 'file';

SELECT splitByChar('\n', description)[1] = '# Local File dictionary source'
FROM system.documentation
WHERE type = 'Dictionary Source' AND name = 'file';

SELECT position(description, '**Syntax**') = 0
FROM system.documentation
WHERE type = 'Dictionary Source' AND name = 'file';
