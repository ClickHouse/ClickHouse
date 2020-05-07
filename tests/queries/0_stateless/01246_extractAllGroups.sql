-- error cases
SELECT extractAllGroups();  --{serverError 42} not enough arguments
SELECT extractAllGroups('hello');  --{serverError 42} not enough arguments
SELECT extractAllGroups('hello', 123);  --{serverError 43} invalid argument type
SELECT extractAllGroups(123, 'world');  --{serverError 43}  invalid argument type
SELECT extractAllGroups('hello world', '((('); --{serverError 427}  invalid re
SELECT extractAllGroups('hello world', materialize('\\w+')); --{serverError 44} non-const needle

SELECT '0 groups, zero matches';
SELECT extractAllGroups('hello world', '\\w+');

SELECT '1 group, multiple matches, String and FixedString';
SELECT extractAllGroups('hello world', '(\\w+)');
SELECT extractAllGroups('hello world', CAST('(\\w+)' as FixedString(5)));
SELECT extractAllGroups(CAST('hello world' AS FixedString(12)), '(\\w+)');
SELECT extractAllGroups(CAST('hello world' AS FixedString(12)), CAST('(\\w+)' as FixedString(5)));
SELECT extractAllGroups(materialize(CAST('hello world' AS FixedString(12))), '(\\w+)');
SELECT extractAllGroups(materialize(CAST('hello world' AS FixedString(12))), CAST('(\\w+)' as FixedString(5)));

SELECT 'mutiple groups, multiple matches';
SELECT extractAllGroups('abc=111, def=222, ghi=333 "jkl mno"="444 foo bar"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');

SELECT 'big match';
SELECT
    length(haystack), length(matches[1]), length(matches), arrayMap((x) -> length(x), arrayMap(x -> x[1], matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractAllGroups(haystack, '(abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz)') AS matches
    FROM numbers(3)
);

SELECT 'lots of matches';
SELECT
    length(haystack), length(matches[1]), length(matches), arrayReduce('sum', arrayMap((x) -> length(x), arrayMap(x -> x[1], matches)))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractAllGroups(haystack, '(\\w)') AS matches
    FROM numbers(3)
);

SELECT 'lots of groups';
SELECT
    length(haystack), length(matches[1]), length(matches), arrayMap((x) -> length(x), arrayMap(x -> x[1], matches))
FROM (
    SELECT
        repeat('abcdefghijklmnopqrstuvwxyz', number * 10) AS haystack,
        extractAllGroups(haystack, repeat('(\\w)', 100)) AS matches
    FROM numbers(3)
);
