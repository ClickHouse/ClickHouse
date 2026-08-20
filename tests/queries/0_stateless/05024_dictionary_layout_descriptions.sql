-- Every registered dictionary layout provides documentation for the generated reference pages.
SELECT count() = countIf(notEmpty(trimBoth(description)))
FROM system.dictionary_layouts;
