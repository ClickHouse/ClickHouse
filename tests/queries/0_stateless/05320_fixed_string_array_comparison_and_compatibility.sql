-- Arrays with `FixedString` elements are compared with arrays of another element type element by element,
-- zero-padded, like the scalars, even though the conversion of `FixedString` to `String` keeps the padding.
SELECT 'arrays';
SELECT toFixedString('a', 2) = 'a', [toFixedString('a', 2)] = ['a'], [toFixedString('a', 2)] != ['a'];
SELECT [toFixedString('a', 2)] = [toFixedString('a', 3)], [toFixedString('a', 2)] = ['a\0'], [toFixedString('a', 2)] = ['b'];
SELECT [toFixedString('a', 2)] < ['b'], [toFixedString('a', 2)] < ['a'], [toFixedString('a', 2)] <= ['a'], ['a'] >= [toFixedString('a', 2)];
SELECT [[toFixedString('a', 2)]] = [['a']], [tuple(toFixedString('a', 2))] = [tuple('a')];
SELECT [toFixedString('a', 2)] = [CAST('a', 'Enum8(\'a\' = 1)')], [toNullable(toFixedString('a', 2)), NULL] = ['a', NULL];
SELECT [toFixedString('a', 2), toFixedString('b', 2)] = ['a'], [toFixedString('a', 2)] < ['a', 'b'];
SELECT [toFixedString(toString(number), 3)] = [toString(number)] FROM numbers(3);

-- `multiIf` and `transform` convert their arguments without the query context, but follow the setting.
SELECT 'multiIf and transform';
SELECT
    hex(multiIf(number = 0, toFixedString('a', 2), number = 1, 'y', 'x')),
    hex(transform(number, [0], [toFixedString('a', 2)], 'x'))
FROM numbers(1);

SET cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT
    hex(multiIf(number = 0, toFixedString('a', 2), number = 1, 'y', 'x')),
    hex(transform(number, [0], [toFixedString('a', 2)], 'x'))
FROM numbers(1);
