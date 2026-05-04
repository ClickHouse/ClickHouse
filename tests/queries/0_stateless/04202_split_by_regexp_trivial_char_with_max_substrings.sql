-- Test: exercises the new `splitByRegexp` trivial-char fallback path with the 3-arg
-- (max_substrings) form and the `splitby_max_substrings_includes_remaining_string` setting.
-- Covers: src/Functions/splitByRegexp.cpp:170-171 — buildImpl trivial-char branch
--   `if (patternIsTrivialChar(arguments)) return FunctionFactory::instance().getImpl("splitByChar", context)->build(arguments);`
-- The PR's own tests only cover the 2-argument form for trivial chars; the 3-argument
-- form (with max_substrings) and the splitby_max_substrings_includes_remaining_string
-- setting were not exercised through the new fallback. If the resolver ever drops the
-- 3rd argument or fails to propagate the setting context, results would silently differ
-- from the regex-based behavior.

SELECT '-- 3-arg trivial-char fallback (max_substrings, default setting)';
SELECT splitByRegexp(' ', 'a b c d', -1);
SELECT splitByRegexp(' ', 'a b c d', 0);
SELECT splitByRegexp(' ', 'a b c d', 1);
SELECT splitByRegexp(' ', 'a b c d', 2);
SELECT splitByRegexp(' ', 'a b c d', 3);
SELECT splitByRegexp(' ', 'a b c d', 4);
SELECT splitByRegexp(' ', 'a b c d', 5);

SELECT '-- 3-arg trivial-char fallback (max_substrings, include_remaining_string=1)';
SELECT splitByRegexp(' ', 'a b c d', 1) SETTINGS splitby_max_substrings_includes_remaining_string = 1;
SELECT splitByRegexp(' ', 'a b c d', 2) SETTINGS splitby_max_substrings_includes_remaining_string = 1;
SELECT splitByRegexp(' ', 'a b c d', 3) SETTINGS splitby_max_substrings_includes_remaining_string = 1;
SELECT splitByRegexp(' ', 'a b c d', 4) SETTINGS splitby_max_substrings_includes_remaining_string = 1;

SELECT '-- non-const 2nd argument flowing through trivial-char fallback';
SELECT splitByRegexp(' ', materialize('a b c'), 2);
SELECT splitByRegexp('-', s, 2) FROM (SELECT arrayJoin(['1-2-3', 'x-y-z']) AS s) ORDER BY 1;

SELECT '-- equivalence with splitByChar across all branches';
SELECT splitByRegexp('-', 'a-b-c-d', 2) = splitByChar('-', 'a-b-c-d', 2);
SELECT splitByRegexp(' ', '', 5) = splitByChar(' ', '', 5);
SELECT splitByRegexp(' ', 'no separators', 3) = splitByChar(' ', 'no separators', 3);
SELECT splitByRegexp(' ', 'a  b', 5) = splitByChar(' ', 'a  b', 5);
