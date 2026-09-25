-- Semantics of `hasAnyTokenPrefix`, `hasAnyTokenLike`, `hasAllTokenLike` and `hasAnyTokenRegexp` without a text index.

SELECT '-- hasAnyTokenPrefix';
SELECT hasAnyTokenPrefix('Payment charged twice', 'charg');
SELECT hasAnyTokenPrefix('recharge failed', 'charg');
SELECT hasAnyTokenPrefix('Payment charged twice', 'Charg');
SELECT hasAnyTokenPrefix('Payment charged twice', 'charged');
SELECT hasAnyTokenPrefix('Payment charged twice', 'charged twice');
SELECT hasAnyTokenPrefix('abc', '');
SELECT hasAnyTokenPrefix('', '');
SELECT hasAnyTokenPrefix('!!! ???', '');
SELECT hasAnyTokenPrefix('', 'a');
SELECT hasAnyTokenPrefix('привет мир', 'при');
SELECT hasAnyTokenPrefix('привет мир', 'ми');
SELECT hasAnyTokenPrefix('привет мир', 'ивет');
SELECT hasAnyTokenPrefix('Payment refunded', ['charg', 'refund']);
SELECT hasAnyTokenPrefix('Payment refunded', ['charg', 'fund']);
SELECT hasAnyTokenPrefix('Payment refunded', ['', 'x']);
SELECT hasAnyTokenPrefix('!!! ???', ['', 'x']);
-- `%`, `_` and `\` are matched literally.
SELECT hasAnyTokenPrefix('50% off', '50%', 'splitByString([\' \'])');
SELECT hasAnyTokenPrefix('500 off', '50%', 'splitByString([\' \'])');
SELECT hasAnyTokenPrefix('x_y', 'x_', 'splitByString([\' \'])');
SELECT hasAnyTokenPrefix('xzy', 'x_', 'splitByString([\' \'])');

SELECT '-- hasAnyTokenLike';
SELECT hasAnyTokenLike('Payment charged twice', 'ch%ed');
SELECT hasAnyTokenLike('Payment charged twice', 'charge');
SELECT hasAnyTokenLike('Payment charged twice', '%arge%');
SELECT hasAnyTokenLike('cat hat', '_at');
SELECT hasAnyTokenLike('cat hat', 'c_');
SELECT hasAnyTokenLike('abc', '%');
SELECT hasAnyTokenLike('', '%');
SELECT hasAnyTokenLike('abc', '');
SELECT hasAnyTokenLike('привет мир', 'пр_вет');
-- `\` escapes `%` and `_`. Tokens are split by spaces here, so they keep these characters.
SELECT hasAnyTokenLike('a%b axb', 'a\\%b', 'splitByString([\' \'])');
SELECT hasAnyTokenLike('axb', 'a\\%b', 'splitByString([\' \'])');
SELECT hasAnyTokenLike('a_b', 'a\\_b', 'splitByString([\' \'])');
SELECT hasAnyTokenLike('axb', 'a\\_b', 'splitByString([\' \'])');
SELECT hasAnyTokenLike('a\\b', 'a\\\\b', 'splitByString([\' \'])');
SELECT hasAnyTokenLike('Payment charged twice', ['%ing', 'tw_ce']);
SELECT hasAnyTokenLike('Payment charged twice', ['%ing', 'charge']);
SELECT hasAnyTokenLike('abc', ['']);
-- A String is one pattern, it is not split into tokens.
SELECT hasAnyTokenLike('a b', 'a b');
SELECT hasAnyTokenLike('a b', ['a', 'b']);

SELECT '-- hasAllTokenLike';
SELECT hasAllTokenLike('Payment charged twice', ['ch%', 'tw%']);
SELECT hasAllTokenLike('charged', ['ch%', '%ed']);
SELECT hasAllTokenLike('Payment charged twice', ['ch%', 'refund%']);
SELECT hasAllTokenLike('Payment charged twice', 'ch%');
SELECT hasAllTokenLike('Payment charged twice', 'refund%');
SELECT hasAllTokenLike('abc', ['a%', 'a%']);
SELECT hasAllTokenLike('abc', ['a%', '']);
SELECT hasAllTokenLike(['abc', 'def'], ['a%', 'd%']);
SELECT hasAllTokenLike(['abc', 'def'], ['a%', 'x%']);
SELECT hasAllTokenLike('a b', 'a b');

SELECT '-- hasAnyTokenRegexp';
SELECT hasAnyTokenRegexp('order 12345 shipped', '^[0-9]{5}$');
SELECT hasAnyTokenRegexp('order 123456 shipped', '^[0-9]{5}$');
SELECT hasAnyTokenRegexp('order 123456 shipped', '[0-9]{5}');
SELECT hasAnyTokenRegexp('abc123', '^[a-z]+$');
SELECT hasAnyTokenRegexp('abc123 def', '^[a-z]+$');
SELECT hasAnyTokenRegexp('abc', '');
SELECT hasAnyTokenRegexp('', '');
SELECT hasAnyTokenRegexp('error warning', 'err|fatal');
SELECT hasAnyTokenRegexp('Error', '(?i)^error$');
SELECT hasAnyTokenRegexp('order 123456 shipped', ['^[0-9]{5}$', '^ship']);
SELECT hasAnyTokenRegexp('order 123456 shipped', ['^[0-9]{5}$', '^x']);
SELECT hasAnyTokenRegexp('abc', ['', 'x']);
-- Partial match, unlike LIKE.
SELECT hasAnyTokenRegexp('error', 'rro'), hasAnyTokenLike('error', 'rro');

SELECT '-- empty array';
SELECT hasAnyTokenPrefix('abc', []), hasAnyTokenLike('abc', []), hasAllTokenLike('abc', []), hasAnyTokenRegexp('abc', []);
SELECT hasAnyTokenPrefix(NULL::Nullable(String), []), hasAnyTokenLike(NULL::Nullable(String), []), hasAllTokenLike(NULL::Nullable(String), []), hasAnyTokenRegexp(NULL::Nullable(String), []);
SELECT hasAllTokenLike(toNullable('abc'), []), toTypeName(hasAllTokenLike(toNullable('abc'), []));

SELECT '-- types';
SELECT hasAnyTokenPrefix(toNullable('abc def'), 'de'), toTypeName(hasAnyTokenPrefix(toNullable('abc def'), 'de'));
SELECT hasAnyTokenPrefix(NULL::Nullable(String), 'de');
SELECT hasAnyTokenPrefix('abc', NULL);
SELECT hasAnyTokenLike(toLowCardinality('abc def'), 'd%'), toTypeName(hasAnyTokenLike(toLowCardinality('abc def'), 'd%'));
SELECT hasAnyTokenRegexp(toFixedString('abc def', 10), '^def$');
SELECT hasAnyTokenPrefix(['abc', 'def ghi'], 'gh');
SELECT hasAnyTokenPrefix(['abc', 'def ghi'], 'xy');
SELECT hasAnyTokenPrefix(CAST([], 'Array(String)'), '');
SELECT hasAnyTokenLike([NULL, 'abc def'], 'd_f');
SELECT hasAnyTokenRegexp([toFixedString('ab', 3), toFixedString('cd', 3)], '^cd$');
SELECT hasAllTokenLike(toNullable('abc def'), ['a%', 'd%']), toTypeName(hasAllTokenLike(toNullable('abc def'), ['a%', 'd%']));

SELECT '-- tokenizers';
SELECT hasAnyTokenPrefix('abc-def', 'c-d', 'array');
SELECT hasAnyTokenPrefix('abc-def', 'abc-', 'array');
SELECT hasAnyTokenLike('abcdef', 'bc_', 'ngrams(3)');
SELECT hasAnyTokenRegexp('key=value;flag', '^val', 'splitByString([\'=\', \';\'])');
SELECT hasAnyTokenRegexp('key=value;flag', '^val', 'splitByNonAlpha');
SELECT hasAllTokenLike('abcdef', ['ab_', '_ef'], 'ngrams(3)');

SELECT '-- same as arrayExists over tokens';
SELECT
    countIf(hasAnyTokenPrefix(s, 'ab') != arrayExists(t -> startsWith(t, 'ab'), tokens(s))),
    countIf(hasAnyTokenLike(s, 'a%c') != arrayExists(t -> like(t, 'a%c'), tokens(s))),
    countIf(hasAnyTokenRegexp(s, '^[ab]+c$') != arrayExists(t -> match(t, '^[ab]+c$'), tokens(s))),
    countIf(hasAnyTokenPrefix(s, 'ab'))
FROM (SELECT arrayStringConcat(arrayMap(x -> ['ab', 'abc', 'bac', 'ca', 'aac', ' ', '-'][x % 7 + 1], range(number % 5)), '') AS s FROM numbers(1000));

SELECT
    countIf(hasAnyTokenPrefix(s, ['ab', 'ca']) != arrayExists(t -> arrayExists(p -> startsWith(t, p), ['ab', 'ca']), tokens(s))),
    countIf(hasAnyTokenLike(s, ['a%c', 'b_']) != arrayExists(t -> arrayExists(p -> like(t, p), ['a%c', 'b_']), tokens(s))),
    countIf(hasAllTokenLike(s, ['a%c', 'b%']) != (notEmpty(['a%c', 'b%']) AND arrayAll(p -> arrayExists(t -> like(t, p), tokens(s)), ['a%c', 'b%']))),
    countIf(hasAnyTokenRegexp(s, ['^[ab]+c$', 'ca']) != arrayExists(t -> arrayExists(p -> match(t, p), ['^[ab]+c$', 'ca']), tokens(s))),
    countIf(hasAllTokenLike(s, ['a%c', 'b%']))
FROM (SELECT arrayStringConcat(arrayMap(x -> ['ab', 'abc', 'bac', 'ca', 'aac', ' ', '-'][(x * 3 + number) % 7 + 1], range(number % 6)), '') AS s FROM numbers(1000));

SELECT '-- many blocks and threads with a stateful tokenizer';
SELECT
    countIf(hasAnyTokenPrefix(s, '12', 'sparseGrams(3, 5)') != arrayExists(t -> startsWith(t, '12'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasAnyTokenLike(s, '1_3%', 'sparseGrams(3, 5)') != arrayExists(t -> like(t, '1_3%'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasAnyTokenRegexp(s, '^1.3', 'sparseGrams(3, 5)') != arrayExists(t -> match(t, '^1.3'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasAnyTokenPrefix(s, '12', 'sparseGrams(3, 5)')),
    countIf(hasAnyTokenLike(s, '1_3%', 'sparseGrams(3, 5)')),
    countIf(hasAnyTokenRegexp(s, '^1.3', 'sparseGrams(3, 5)'))
FROM (SELECT toString(number * 7919) AS s FROM numbers_mt(100000))
SETTINGS max_block_size = 100, max_threads = 4;

SELECT
    countIf(hasAnyTokenPrefix(s, ['12', '34'], 'sparseGrams(3, 5)') != arrayExists(t -> startsWith(t, '12') OR startsWith(t, '34'), tokens(s, 'sparseGrams', 3, 5))),
    countIf(hasAllTokenLike(s, ['1_3%', '%9'], 'sparseGrams(3, 5)') != (arrayExists(t -> like(t, '1_3%'), tokens(s, 'sparseGrams', 3, 5)) AND arrayExists(t -> like(t, '%9'), tokens(s, 'sparseGrams', 3, 5)))),
    countIf(hasAnyTokenPrefix(s, ['12', '34'], 'sparseGrams(3, 5)')),
    countIf(hasAllTokenLike(s, ['1_3%', '%9'], 'sparseGrams(3, 5)'))
FROM (SELECT toString(number * 7919) AS s FROM numbers_mt(100000))
SETTINGS max_block_size = 100, max_threads = 4;

SELECT '-- arrays in a full column, the first row included';
SELECT n, hasAnyTokenPrefix(arr, 'ab'), hasAnyTokenLike(arr, 'a_c'), hasAnyTokenRegexp(arr, '^abc$'), hasAllTokenLike(arr, ['a%', 'x%'])
FROM values('n UInt8, arr Array(String)', (0, ['xy', 'abc']), (1, []), (2, ['ab']), (3, ['xy abc']), (4, ['xy'])) ORDER BY n;
SELECT n, hasAnyTokenLike(arr, 'a_c') FROM values('n UInt8, arr Array(Nullable(String))', (0, [NULL, 'abc']), (1, [NULL]), (2, ['xbc'])) ORDER BY n;
SELECT n, hasAnyTokenRegexp(arr, '^cd$') FROM values('n UInt8, arr Array(FixedString(2))', (0, ['ab', 'cd']), (1, ['ab'])) ORDER BY n;

SELECT '-- hasAllTokenLike starts every row afresh';
SELECT n, hasAllTokenLike(s, ['a%', 'x%']) FROM values('n UInt8, s String', (0, 'ab'), (1, 'xy'), (2, 'ab xy')) ORDER BY n;
SELECT n, hasAllTokenLike(arr, ['a%', 'x%']) FROM values('n UInt8, arr Array(String)', (0, ['ab']), (1, ['xy']), (2, ['ab', 'xy'])) ORDER BY n;

SELECT '-- errors';
SELECT hasAnyTokenPrefix('abc'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasAnyTokenPrefix('abc', 'a', 'splitByNonAlpha', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT hasAnyTokenPrefix(1, 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyTokenPrefix('abc', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyTokenPrefix('abc', [NULL, 'a']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyTokenLike('abc', [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAllTokenLike('abc', toFixedString('a', 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyTokenRegexp('abc', [toFixedString('a', 1)]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT hasAnyTokenPrefix('abc', materialize('a')); -- { serverError ILLEGAL_COLUMN }
SELECT hasAllTokenLike('abc', materialize(['a'])); -- { serverError ILLEGAL_COLUMN }
SELECT hasAnyTokenLike('abc', 'a', materialize('splitByNonAlpha')); -- { serverError ILLEGAL_COLUMN }
SELECT hasAnyTokenLike('abc', 'a', 'unknownTokenizer'); -- { serverError BAD_ARGUMENTS }
SELECT hasAnyTokenRegexp('abc', '('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT hasAnyTokenRegexp('abc', ['a', '(']); -- { serverError CANNOT_COMPILE_REGEXP }
