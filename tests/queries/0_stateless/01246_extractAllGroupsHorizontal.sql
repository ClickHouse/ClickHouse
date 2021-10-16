-- error cases
SELECT extractAllGroupsHorizontal();  --{serverError 42} not enough arguments
SELECT extractAllGroupsHorizontal('hello');  --{serverError 42} not enough arguments
SELECT extractAllGroupsHorizontal('hello', 123);  --{serverError 43} invalid argument type
SELECT extractAllGroupsHorizontal(123, 'world');  --{serverError 43}  invalid argument type
SELECT extractAllGroupsHorizontal('hello world', '((('); --{serverError 427}  invalid re
SELECT extractAllGroupsHorizontal('hello world', materialize('\\w+')); --{serverError 44} non-cons needle

SELECT '0 groups, zero matches';
SELECT extractAllGroupsHorizontal('hello world', '\\w+');  -- { serverError 36 }

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
