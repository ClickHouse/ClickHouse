-- A capture group that does not participate in the match is returned by re2 as an empty piece
-- with a null `data()`. Substituting it reached `memcpy(dst, nullptr, 0)` in `ReplaceRegexpImpl`,
-- which UBSan reports as "null pointer passed as argument 2, which is declared to never be null"
-- (memcpy's pointers are `nonnull` even for a zero length). The JIT path already guarded this;
-- the re2 path did not. The results below are unchanged by the fix - only the UB is gone.

SELECT '-- an optional group that never participates --';
SELECT replaceRegexpAll('abc', '(x)?b', '[\\1]');
SELECT replaceRegexpOne('abc', '(x)?b', '[\\1]');

SELECT '-- the group is in the branch of an alternation that was not taken --';
SELECT replaceRegexpAll('aaa', '(a)|(z)', '<\\1\\2>');
SELECT replaceRegexpOne('aaa', '(a)|(z)', '<\\1\\2>');

SELECT '-- the replacement is nothing but the unmatched group --';
SELECT replaceRegexpAll('abc', '(x)?b', '\\1');
SELECT replaceRegexpAll('abc', 'b', '');

SELECT '-- the same unmatched group substituted several times --';
SELECT replaceRegexpAll('abc', '(x)?b', '\\1\\1\\1');

SELECT '-- matched and unmatched groups mixed, with literals in between --';
SELECT replaceRegexpAll('abc', '(a)(x)?(b)', '[\\1|\\2|\\3]');

SELECT '-- the whole match next to an unmatched group --';
SELECT replaceRegexpAll('abc', '(x)?b', '<\\0|\\1>');

SELECT '-- five groups, none of which participate (the pattern from 04650) --';
SELECT replaceRegexpAll('123', '[0-9]((a|b)(c|d)|(e|f)(g|h))?', '[\\1\\2\\3\\4\\5]');

SELECT '-- nested optional groups --';
SELECT replaceRegexpAll('xy', '((p)(q))?x', '[\\1|\\2|\\3]');

SELECT '-- an empty haystack, and an empty match at the end of the string --';
SELECT replaceRegexpAll('', '(x)?', '[\\1]');
SELECT replaceRegexpAll('ab', '(x)?$', '[\\1]');
SELECT replaceRegexpAll('ab', '(x)?', '[\\1]');

SELECT '-- a trailing group that matches only sometimes, over several rows --';
SELECT replaceRegexpAll(s, '(a)(b)?', '[\\1|\\2]')
FROM (SELECT arrayJoin(['a', 'ab', 'aab', 'ba', '']) AS s);

SELECT '-- non-constant haystack (the vector path) --';
SELECT replaceRegexpAll(materialize('abc'), '(x)?b', '[\\1]') FROM numbers(2);

SELECT '-- non-constant needle --';
SELECT replaceRegexpAll(materialize('abc'), materialize('(x)?b'), '[\\1]') FROM numbers(2);

SELECT '-- non-constant replacement --';
SELECT replaceRegexpAll(materialize('abc'), '(x)?b', materialize('[\\1]')) FROM numbers(2);

SELECT '-- non-constant haystack, needle and replacement --';
SELECT replaceRegexpAll(materialize('abc'), materialize('(x)?b'), materialize('[\\1]')) FROM numbers(2);

SELECT '-- FixedString haystack (a constant FixedString is not accepted by the function) --';
SELECT replaceRegexpAll(materialize(toFixedString('abc', 3)), '(x)?b', '[\\1]') FROM numbers(2);

SELECT '-- forced through the re2 path --';
SELECT replaceRegexpAll(materialize('abc'), '(x)?b', '[\\1]')
FROM numbers(8) SETTINGS compile_regular_expressions = 0;

SELECT '-- forced through the JIT path, which compiles after one use --';
SELECT replaceRegexpAll(materialize('abc'), '(x)?b', '[\\1]')
FROM numbers(8) SETTINGS compile_regular_expressions = 1, min_count_to_compile_regular_expression = 1;

SELECT '-- and the same for replaceRegexpOne on both paths --';
SELECT replaceRegexpOne(materialize('abcb'), '(x)?b', '[\\1]')
FROM numbers(4) SETTINGS compile_regular_expressions = 0;
SELECT replaceRegexpOne(materialize('abcb'), '(x)?b', '[\\1]')
FROM numbers(4) SETTINGS compile_regular_expressions = 1, min_count_to_compile_regular_expression = 1;
