-- error cases
SELECT extractAllGroupsHorizontal();  --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} not enough arguments
SELECT extractAllGroupsHorizontal('hello');  --{serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH} not enough arguments
SELECT extractAllGroupsHorizontal('hello', 123);  --{serverError ILLEGAL_TYPE_OF_ARGUMENT} invalid argument type
SELECT extractAllGroupsHorizontal(123, 'world');  --{serverError ILLEGAL_TYPE_OF_ARGUMENT}  invalid argument type
SELECT extractAllGroupsHorizontal('hello world', '((('); --{serverError CANNOT_COMPILE_REGEXP}  invalid re
SELECT extractAllGroupsHorizontal('hello world', materialize('\\w+')); --{serverError ILLEGAL_COLUMN} non-cons needle
SELECT extractAllGroupsHorizontal('hello world', '\\w+');  -- { serverError BAD_ARGUMENTS } 0 groups
SELECT extractAllGroupsHorizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 0;  -- { serverError TOO_LARGE_ARRAY_SIZE } to many groups matched per row
SELECT extractAllGroupsHorizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 1;  -- { serverError TOO_LARGE_ARRAY_SIZE } to many groups matched per row

SELECT extractAllGroupsHorizontal('hello world', '(\\w+)') SETTINGS regexp_max_matches_per_row = 1000000 FORMAT Null; -- users now can set limit bigger than previous 1000 matches per row

SELECT '1 group, multiple matches, String and FixedString';
SELECT extractAllGroupsHorizontal('hello world', '(\\w+)');
SELECT extractAllGroupsHorizontal('hello world', CAST('(\\w+)' as FixedString(5)));
SELECT extractAllGroupsHorizontal(CAST('hello world' AS FixedString(12)), '(\\w+)');
SELECT extractAllGroupsHorizontal(CAST('hello world' AS FixedString(12)), CAST('(\\w+)' as FixedString(5)));
SELECT extractAllGroupsHorizontal(materialize(CAST('hello world' AS FixedString(12))), '(\\w+)');
SELECT extractAllGroupsHorizontal(materialize(CAST('hello world' AS FixedString(12))), CAST('(\\w+)' as FixedString(5)));

SELECT 'mutiple groups, multiple matches';
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches), length(matches[1]), arrayMap((x) -> length(x), matches[1])
FROM (
    SELECT
           repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
           extractAllGroupsHorizontal(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches), length(matches[1]), arrayReduce('sum', arrayMap((x) -> length(x), matches[1]))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractAllGroupsHorizontal(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches), length(matches[1]), arrayMap((x) -> length(x), matches[1])
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractAllGroupsHorizontal(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
