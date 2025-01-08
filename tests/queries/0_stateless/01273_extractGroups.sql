-- error cases
SELECT extractGroups();  --{serverError 42} not enough arguments
SELECT extractGroups('hello');  --{serverError 42} not enough arguments
SELECT extractGroups('hello', 123);  --{serverError 43} invalid argument type
SELECT extractGroups(123, 'world');  --{serverError 43}  invalid argument type
SELECT extractGroups('hello world', '((('); --{serverError 427}  invalid re
SELECT extractGroups('hello world', materialize('\\w+')); --{serverError 44} non-const needle

SELECT '0 groups, zero matches';
SELECT extractGroups('hello world', '\\w+'); -- { serverError 36 }

SELECT '1 group, multiple matches, String and FixedString';
SELECT extractGroups('hello world', '(\\w+) (\\w+)');
SELECT extractGroups('hello world', CAST('(\\w+) (\\w+)' as FixedString(11)));
SELECT extractGroups(CAST('hello world' AS FixedString(12)), '(\\w+) (\\w+)');
SELECT extractGroups(CAST('hello world' AS FixedString(12)), CAST('(\\w+) (\\w+)' as FixedString(11)));
SELECT extractGroups(materialize(CAST('hello world' AS FixedString(12))), '(\\w+) (\\w+)');
SELECT extractGroups(materialize(CAST('hello world' AS FixedString(12))), CAST('(\\w+) (\\w+)' as FixedString(11)));

SELECT 'multiple matches';
SELECT extractGroups('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches), arrayMap((x) -> length(x), matches)
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractGroups(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches), arrayReduce('sum', arrayMap((x) -> length(x), matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractGroups(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches), arrayMap((x) -> length(x), matches)
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractGroups(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
