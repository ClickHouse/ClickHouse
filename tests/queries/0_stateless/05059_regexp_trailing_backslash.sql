-- Regression for #117853.
-- A trailing unescaped backslash is an invalid RE2 regexp and must not
-- be treated as a trivial substring.

SELECT match('abcd', 'abc\\'); -- { serverError 427 }
SELECT match('abcd', '\\'); -- { serverError 427 }

SELECT extractAll('abcd', 'abc\\'); -- { serverError 427 }
SELECT countMatches('abcabc', 'abc\\'); -- { serverError 427 }
SELECT splitByRegexp('abc\\', 'xabcy'); -- { serverError 427 }

-- Valid escaped backslashes must continue working.
SELECT match('abc\\', 'abc\\\\');
SELECT extractAll('abc\\', 'abc\\\\');
SELECT countMatches('abc\\abc\\', 'abc\\\\');
SELECT splitByRegexp('abc\\\\', 'xabc\\y');
