SET enable_analyzer = 1;

-- The `splitByNonAlpha` tokenizer keeps non-ASCII bytes, even invalid UTF-8.
-- It skips empty tokens and preserves token order and duplicates.
SELECT 'empty, ASCII, Unicode, and binary strings';
SELECT hex(s), arrayMap(hex, tokens(s, 'splitByNonAlpha'))
FROM values('s String', '', ' \t\n\r\0_!\x7f', '09AZaz', '/09:AZ[az{',
    'a__a\0b\x7fc', 'é中🙂 x', '\x80\xbf\xc0\xff', 'a\xff_b\x80')
ORDER BY s;

-- Each possible byte appears in every lane, including the scalar prefix and tail.
-- The expectation comes from the byte ranges, independently of the tokenizer.
SELECT 'all byte values at offsets 0 through 64';
WITH
    concat(repeat('a', offset), char(byte), repeat('b', 33)) AS s,
    byte >= 128 OR byte BETWEEN 48 AND 57 OR byte BETWEEN 65 AND 90 OR byte BETWEEN 97 AND 122 AS is_token,
    if(is_token, [s], arrayFilter(x -> notEmpty(x), [repeat('a', offset), repeat('b', 33)])) AS expected
SELECT count(), countIf(tokens(s, 'splitByNonAlpha') != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS offset FROM numbers(65)) AS offsets;

SELECT 'homogeneous tokens and separator runs of lengths 0 through 64';
WITH
    repeat(char(byte), size) AS s,
    byte >= 128 OR byte BETWEEN 48 AND 57 OR byte BETWEEN 65 AND 90 OR byte BETWEEN 97 AND 122 AS is_token,
    if(is_token AND size > 0, [s], []) AS expected
SELECT count(), countIf(tokens(s, 'splitByNonAlpha') != expected)
FROM (SELECT number AS byte FROM numbers(256)) AS bytes
CROSS JOIN (SELECT number AS size FROM numbers(65)) AS sizes;

SELECT 'tokens separated by runs spanning multiple blocks';
WITH
    concat(repeat('_', gap), repeat('a', size), repeat('_', gap), 'b', repeat('_', gap)) AS s,
    if(gap = 0, [concat(repeat('a', size), 'b')], arrayFilter(x -> notEmpty(x), [repeat('a', size), 'b'])) AS expected
SELECT count(), countIf(tokens(s, 'splitByNonAlpha') != expected)
FROM (SELECT number AS size FROM numbers(65)) AS sizes
CROSS JOIN (SELECT number AS gap FROM numbers(65)) AS gaps;

-- Bytes from the next row and the padding of the last row must not enter a token.
SELECT 'adjacent rows and fixed strings';
SELECT id, arrayMap(hex, tokens(s, 'splitByNonAlpha'))
FROM values('id UInt8, s String', (1, 'aaaaaaaaaaaaaaa'), (2, 'bbbbbbbbbbbbbbbb'),
    (3, ''), (4, '________________'), (5, '\xff\xff'), (6, 'z'))
ORDER BY id;
SELECT arrayMap(hex, tokens(materialize(toFixedString('a\0b', 17)), 'splitByNonAlpha'));
SELECT tokens(materialize(toFixedString('', 33)), 'splitByNonAlpha');
SELECT tokens(toLowCardinality(materialize('a_b_a')), 'splitByNonAlpha');
SELECT tokens(materialize(NULL::Nullable(String)), 'splitByNonAlpha');

-- String needles exercise `nextInString`; the haystack uses `forEachToken`.
-- Long tokens and separator runs also exercise vectorized positive and negative searches.
SELECT 'string and array needles, early matches, last matches, and misses';
WITH
    repeat('a', 49) AS a,
    repeat('b', 33) AS b,
    concat(repeat('_', 33), a, repeat(' ', 49), b, '\0\xff\x80', repeat('_', 33)) AS needle
SELECT id,
    hasAllTokens(s, needle, 'splitByNonAlpha'),
    hasAllTokens(s, [a, b, '\xff\x80'], 'splitByNonAlpha'),
    hasAnyTokens(s, needle, 'splitByNonAlpha'),
    hasAnyTokens(s, [a, b, '\xff\x80'], 'splitByNonAlpha')
FROM
(
    SELECT number AS id, arrayElement(['', a, b, '\xff\x80', concat(a, '_', b, '_\xff\x80'),
        concat('\xff\x80_', b, '_', a), concat(a, '_', a), concat('x', a), 'missing'], number + 1) AS s
    FROM numbers(9)
)
ORDER BY id;
