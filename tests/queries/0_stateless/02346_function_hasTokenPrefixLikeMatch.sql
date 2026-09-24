-- Semantics of `hasTokenPrefix`, `hasTokenLike` and `hasTokenMatch` without a text index.

SELECT '-- hasTokenPrefix';
SELECT hasTokenPrefix('Payment charged twice', 'charg');
SELECT hasTokenPrefix('recharge failed', 'charg');
SELECT hasTokenPrefix('Payment charged twice', 'Charg');
SELECT hasTokenPrefix('Payment charged twice', 'charged');
SELECT hasTokenPrefix('Payment charged twice', 'charged twice');
SELECT hasTokenPrefix('abc', '');
SELECT hasTokenPrefix('', '');
SELECT hasTokenPrefix('!!! ???', '');
SELECT hasTokenPrefix('', 'a');
SELECT hasTokenPrefix('привет мир', 'при');
SELECT hasTokenPrefix('привет мир', 'ми');
SELECT hasTokenPrefix('привет мир', 'ивет');

SELECT '-- hasTokenLike';
SELECT hasTokenLike('Payment charged twice', 'ch%ed');
SELECT hasTokenLike('Payment charged twice', 'charge');
SELECT hasTokenLike('Payment charged twice', '%arge%');
SELECT hasTokenLike('cat hat', '_at');
SELECT hasTokenLike('cat hat', 'c_');
SELECT hasTokenLike('abc', '%');
SELECT hasTokenLike('', '%');
SELECT hasTokenLike('abc', '');
SELECT hasTokenLike('привет мир', 'пр_вет');
-- `\` escapes `%` and `_`. Tokens are split by spaces here, so they keep these characters.
SELECT hasTokenLike('a%b axb', 'a\\%b', 'splitByString([\' \'])');
SELECT hasTokenLike('axb', 'a\\%b', 'splitByString([\' \'])');
SELECT hasTokenLike('a_b', 'a\\_b', 'splitByString([\' \'])');
SELECT hasTokenLike('axb', 'a\\_b', 'splitByString([\' \'])');
SELECT hasTokenLike('a\\b', 'a\\\\b', 'splitByString([\' \'])');

SELECT '-- hasTokenMatch';
SELECT hasTokenMatch('order 12345 shipped', '^[0-9]{5}$');
SELECT hasTokenMatch('order 123456 shipped', '^[0-9]{5}$');
SELECT hasTokenMatch('order 123456 shipped', '[0-9]{5}');
SELECT hasTokenMatch('abc123', '^[a-z]+$');
SELECT hasTokenMatch('abc123 def', '^[a-z]+$');
SELECT hasTokenMatch('abc', '');
SELECT hasTokenMatch('', '');
SELECT hasTokenMatch('error warning', 'err|fatal');
SELECT hasTokenMatch('Error', '(?i)^error$');

SELECT '-- types';
SELECT hasTokenPrefix(toNullable('abc def'), 'de'), toTypeName(hasTokenPrefix(toNullable('abc def'), 'de'));
SELECT hasTokenPrefix(NULL::Nullable(String), 'de');
SELECT hasTokenPrefix('abc', NULL);
SELECT hasTokenLike(toLowCardinality('abc def'), 'd%'), toTypeName(hasTokenLike(toLowCardinality('abc def'), 'd%'));
SELECT hasTokenMatch(toFixedString('abc def', 10), '^def$');
SELECT hasTokenPrefix(['abc', 'def ghi'], 'gh');
SELECT hasTokenPrefix(['abc', 'def ghi'], 'xy');
SELECT hasTokenPrefix(CAST([], 'Array(String)'), '');
SELECT hasTokenLike([NULL, 'abc def'], 'd_f');
SELECT hasTokenMatch([toFixedString('ab', 3), toFixedString('cd', 3)], '^cd$');

SELECT '-- tokenizers';
SELECT hasTokenPrefix('abc-def', 'c-d', 'array');
SELECT hasTokenPrefix('abc-def', 'abc-', 'array');
SELECT hasTokenLike('abcdef', 'bc_', 'ngrams(3)');
SELECT hasTokenMatch('key=value;flag', '^val', 'splitByString([\'=\', \';\'])');
SELECT hasTokenMatch('key=value;flag', '^val', 'splitByNonAlpha');

SELECT '-- same as arrayExists over tokens';
SELECT
    countIf(hasTokenPrefix(s, 'ab') != arrayExists(t -> startsWith(t, 'ab'), tokens(s))),
    countIf(hasTokenLike(s, 'a%c') != arrayExists(t -> like(t, 'a%c'), tokens(s))),
    countIf(hasTokenMatch(s, '^[ab]+c$') != arrayExists(t -> match(t, '^[ab]+c$'), tokens(s))),
    countIf(hasTokenPrefix(s, 'ab'))
FROM (SELECT arrayStringConcat(arrayMap(x -> ['ab', 'abc', 'bac', 'ca', 'aac', ' ', '-'][x % 7 + 1], range(number % 5)), '') AS s FROM numbers(1000));

SELECT '-- many blocks and threads with a stateful tokenizer';
SELECT
    countIf(hasTokenPrefix(s, '12', 'sparseGrams(3, 5)') != arrayExists(t -> startsWith(t, '12'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasTokenLike(s, '1_3%', 'sparseGrams(3, 5)') != arrayExists(t -> like(t, '1_3%'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasTokenMatch(s, '^1.3', 'sparseGrams(3, 5)') != arrayExists(t -> match(t, '^1.3'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasTokenPrefix(s, '12', 'sparseGrams(3, 5)')),
    countIf(hasTokenLike(s, '1_3%', 'sparseGrams(3, 5)')),
    countIf(hasTokenMatch(s, '^1.3', 'sparseGrams(3, 5)'))
FROM (SELECT toString(number * 7919) AS s FROM numbers_mt(100000))
SETTINGS max_block_size = 100, max_threads = 4;

SELECT '-- arrays in a full column, the first row included';
SELECT n, hasTokenPrefix(arr, 'ab'), hasTokenLike(arr, 'a_c'), hasTokenMatch(arr, '^abc$')
FROM values('n UInt8, arr Array(String)', (0, ['xy', 'abc']), (1, []), (2, ['ab']), (3, ['xy abc']), (4, ['xy'])) ORDER BY n;
SELECT n, hasTokenLike(arr, 'a_c') FROM values('n UInt8, arr Array(Nullable(String))', (0, [NULL, 'abc']), (1, [NULL]), (2, ['xbc'])) ORDER BY n;
SELECT n, hasTokenMatch(arr, '^cd$') FROM values('n UInt8, arr Array(FixedString(2))', (0, ['ab', 'cd']), (1, ['ab'])) ORDER BY n;

SELECT '-- errors';
SELECT hasTokenPrefix('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasTokenPrefix('abc', 'a', 'splitByNonAlpha', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasTokenPrefix(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenPrefix('abc', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenPrefix('abc', ['a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasTokenPrefix('abc', materialize('a')); -- { serverError ILLEGAL_COLUMN }
SELECT hasTokenLike('abc', 'a', materialize('splitByNonAlpha')); -- { serverError ILLEGAL_COLUMN }
SELECT hasTokenLike('abc', 'a', 'unknownTokenizer'); -- { serverError BAD_ARGUMENTS }
SELECT hasTokenMatch('abc', '('); -- { serverError CANNOT_COMPILE_REGEXP }
