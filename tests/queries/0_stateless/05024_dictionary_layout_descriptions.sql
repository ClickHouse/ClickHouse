-- Every registered dictionary layout provides documentation for the generated reference pages.
SELECT count() = countIf(notEmpty(trimBoth(description)))
FROM system.dictionary_layouts;

-- An H1 identifies the description as a complete page, so `system.documentation` preserves it without
-- appending synthetic sections from the structured documentation fields.
SELECT splitByChar('\n', description)[1] = '# flat dictionary layout'
FROM system.documentation
WHERE type = 'Dictionary Layout' AND name = 'flat';

SELECT position(description, '**Syntax**') = 0 AND position(description, '**Related:**') = 0
FROM system.documentation
WHERE type = 'Dictionary Layout' AND name = 'flat';
