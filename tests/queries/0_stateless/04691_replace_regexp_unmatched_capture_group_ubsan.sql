-- A capturing group that does not participate in the match is reported by re2 as a null string_view.
-- Substituting such a group must produce an empty string, and must not pass a null pointer to memcpy.

SELECT replaceRegexpAll('abc', '(a)|(b)', '[\\2]');
SELECT replaceRegexpOne('abc', '(a)|(b)', '[\\2]');
SELECT replaceRegexpAll('abc', '(x)?b', '[\\1]');
SELECT replaceRegexpAll('abc', '(a)|(b)', '[\\1][\\2]');
SELECT replaceRegexpAll('', '(a)?', '[\\1]');

SELECT replaceRegexpAll(materialize('abc'), '(a)|(b)', '[\\2]');
SELECT replaceRegexpOne(materialize('abc'), materialize('(a)|(b)'), materialize('[\\2]'));
SELECT replaceRegexpAll(materialize('abc'), materialize('(a)|(b)'), '[\\2]');

-- The same through the JIT-compiled matcher.
SELECT replaceRegexpAll(materialize('abc'), '(a)|(b)', '[\\2]') FROM numbers(8) SETTINGS min_count_to_compile_regular_expression = 0;
