-- Regression for #117853.
-- A trailing unescaped backslash is an invalid RE2 regexp and must not
-- be treated as a trivial substring.

SELECT match('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('abcd', '\\'); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT extractAll('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT countMatches('abcabc', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT splitByRegexp('abc\\', 'xabcy'); -- { serverError CANNOT_COMPILE_REGEXP }

-- Valid escaped backslashes must continue working.
SELECT match('abc\\', 'abc\\\\');
SELECT extractAll('abc\\', 'abc\\\\');
SELECT countMatches('abc\\abc\\', 'abc\\\\');
SELECT splitByRegexp('abc\\\\', 'xabc\\y');
