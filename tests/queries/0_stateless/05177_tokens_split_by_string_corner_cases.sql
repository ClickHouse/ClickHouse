SET enable_analyzer = 1;

SELECT 'NUL separators, binary strings, and duplicate separators';
SELECT arrayMap(hex, tokens(materialize('a\0b\0\0c\0'), 'splitByString', ['\0']));
SELECT arrayMap(hex, tokens(materialize('a\0:b\0\0:c\0:'), 'splitByString', ['\0:']));
SELECT arrayMap(hex, tokens(materialize('a\x80b\xffc\x80'), 'splitByString', ['\x80', '\xff', '\x80']));
SELECT tokens(materialize('a, b,,c, '), 'splitByString', [', ', ',', ', ']);
SELECT tokens(materialize('a b\tc\nd'), 'splitByString');

-- Byte membership must not confuse bytes sharing just one nibble.
SELECT 'all byte values at offsets 0 through 64';
WITH
    ['\0', '\x10', ' ', '0', '@', 'P', '`', 'p'] AS separators,
    concat(repeat('x', offset), char(byte), repeat('y', 33)) AS s,
    if(has(separators, char(byte)), arrayFilter(x -> notEmpty(x), [repeat('x', offset), repeat('y', 33)]), [s]) AS expected
SELECT count(), countIf(tokens(s, 'splitByString', separators) != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS offset FROM numbers(65)) AS offsets;

-- Eight distinct high nibbles fit in the lookup; a ninth requires scalar classification.
SELECT 'more than eight high nibbles, duplicates, and bytes added after the ninth nibble';
WITH
    ['\0', '\x10', ' ', '0', '@', 'P', '`', 'p', '\x80', '\x80', '\x81', '\xff', '\x01'] AS separators,
    concat(repeat('x', offset), char(byte), repeat('y', 33)) AS s,
    if(has(separators, char(byte)), arrayFilter(x -> notEmpty(x), [repeat('x', offset), repeat('y', 33)]), [s]) AS expected
SELECT count(), countIf(tokens(s, 'splitByString', separators) != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS offset FROM numbers(65)) AS offsets;

SELECT 'all bytes are separators';
WITH (SELECT arrayMap(x -> char(x), range(256))) AS separators
SELECT count(), countIf(notEmpty(tokens(repeat(char(number % 256), number % 65), 'splitByString', separators)))
FROM numbers(16640);

SELECT 'separators sharing a first byte, with a missing or different last byte';
WITH
    ['\x80!', '\x80?', '\xff!'] AS separators,
    concat(repeat('x', offset), char(byte), '!', repeat('y', 33)) AS s,
    if(byte IN (128, 255), arrayFilter(x -> notEmpty(x), [repeat('x', offset), repeat('y', 33)]), [s]) AS expected
SELECT count(), countIf(tokens(s, 'splitByString', separators) != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS offset FROM numbers(65)) AS offsets;

SELECT 'multi-byte separators with more than eight distinct first-byte high nibbles';
WITH
    ['\0!', '\x10!', ' !', '0!', '@!', 'P!', '`!', 'p!', '\x80!', '\xff!'] AS separators,
    concat(repeat('x', offset), char(byte), '!', repeat('y', 33)) AS s,
    if(has(separators, concat(char(byte), '!')), arrayFilter(x -> notEmpty(x), [repeat('x', offset), repeat('y', 33)]), [s]) AS expected
SELECT count(), countIf(tokens(s, 'splitByString', separators) != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS offset FROM numbers(65)) AS offsets;

SELECT 'long separators at every alignment, including incomplete suffixes';
WITH
    concat(repeat('=', 40), '>') AS separator,
    repeat('x', offset) AS prefix,
    concat(prefix, separator, separator, 'y', separator, repeat('=', 40)) AS s,
    arrayFilter(x -> notEmpty(x), [prefix, 'y', repeat('=', 40)]) AS expected
SELECT count(), countIf(tokens(s, 'splitByString', [separator]) != expected)
FROM (SELECT number AS offset FROM numbers(65));

-- A separator consumes its whole match, including other candidates inside it.
-- The first matching separator in list order wins, even when a later one is longer.
SELECT 'overlapping separators in both priority orders';
WITH repeat('x', number) AS prefix
SELECT count(),
    countIf(tokens(concat(prefix, 'ababaY'), 'splitByString', ['aba', 'ab']) != arrayFilter(x -> notEmpty(x), [prefix, 'baY'])),
    countIf(tokens(concat(prefix, 'ababaY'), 'splitByString', ['ab', 'aba']) != arrayFilter(x -> notEmpty(x), [prefix, 'aY'])),
    countIf(tokens(concat(prefix, 'aabY'), 'splitByString', ['aab', 'ab']) != arrayFilter(x -> notEmpty(x), [prefix, 'Y'])),
    countIf(tokens(concat(prefix, 'aabY'), 'splitByString', ['ab', 'aab']) != arrayFilter(x -> notEmpty(x), [prefix, 'Y']))
FROM numbers(65);

SELECT 'UTF-8 separators across every block alignment';
WITH repeat('x', number) AS prefix
SELECT count(), countIf(tokens(concat(prefix, '🙂中🙂é🙂'), 'splitByString', ['🙂']) != arrayFilter(x -> notEmpty(x), [prefix, '中', 'é']))
FROM numbers(65);

-- No match may use the following row or the padding beyond the string length.
SELECT 'adjacent rows and fixed strings';
SELECT id, arrayMap(hex, tokens(s, 'splitByString', ['=>']))
FROM values('id UInt8, s String', (1, 'aaaaaaaaaaaaaaa='), (2, '>bbbbbbbbbbbbbbb'),
    (3, ''), (4, '=>'), (5, 'ccccccccccccccc='), (6, '>'), (7, '='))
ORDER BY id;
SELECT arrayMap(hex, tokens(materialize(toFixedString('a\0b', 17)), 'splitByString', ['\0']));
SELECT arrayMap(hex, tokens(materialize(toFixedString('a\0b', 17)), 'splitByString', ['\0\0']));
SELECT arrayMap(hex, tokens(materialize(toFixedString('a,b', 17)), 'splitByString', [',']));
SELECT tokens(toLowCardinality(materialize('a::b::a')), 'splitByString', ['::']);
SELECT tokens(materialize(NULL::Nullable(String)), 'splitByString', ['::']);

-- String needles use `nextInString`, including its scalar prefix and vector loop.
-- Array needles provide an independent check of their tokenization.
SELECT 'string and array needles, early matches, last matches, and misses';
WITH
    repeat('x', 49) AS x,
    repeat('y', 33) AS y,
    concat(repeat('::', 25), x, repeat('::', 25), y, '::z', repeat('::', 25)) AS needle
SELECT id,
    hasAllTokens(s, needle, 'splitByString([\'::\'])'),
    hasAllTokens(s, [x, y, 'z'], 'splitByString([\'::\'])'),
    hasAnyTokens(s, needle, 'splitByString([\'::\'])'),
    hasAnyTokens(s, [x, y, 'z'], 'splitByString([\'::\'])')
FROM
(
    SELECT number AS id, arrayElement(['', x, y, 'z', concat(x, '::', y, '::z'),
        concat('z::', y, '::', x), concat(x, '::', x), concat('a', x), 'missing'], number + 1) AS s
    FROM numbers(9)
)
ORDER BY id;

SELECT 'single-byte separators in string needles';
WITH concat(repeat(' ', 49), repeat('x', 49), repeat(',', 33), repeat('y', 33), repeat(' ', 49)) AS needle
SELECT hasAllTokens(materialize(concat(repeat('x', 49), ',', repeat('y', 33))), needle, 'splitByString([\' \', \',\'])'),
    hasAllTokens(materialize(repeat('x', 49)), needle, 'splitByString([\' \', \',\'])'),
    hasAnyTokens(materialize(repeat('y', 33)), needle, 'splitByString([\' \', \',\'])');

SELECT 'scalar classification of string needles with more than eight high nibbles';
WITH concat(repeat('\x80', 49), repeat('x', 49), '\xff', repeat('y', 33), repeat('\0', 49)) AS needle
SELECT hasAllTokens(materialize(concat(repeat('x', 49), '\xff', repeat('y', 33))), needle,
    'splitByString([\'\0\', \'\x10\', \' \', \'0\', \'@\', \'P\', \'`\', \'p\', \'\x80\', \'\xff\'])'),
    hasAllTokens(materialize(repeat('x', 49)), needle,
    'splitByString([\'\0\', \'\x10\', \' \', \'0\', \'@\', \'P\', \'`\', \'p\', \'\x80\', \'\xff\'])');
