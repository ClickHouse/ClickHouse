-- `arrayStringConcat` over `FixedString` elements follows `cast_fixed_string_to_string_strip_trailing_zeros`,
-- the same as `CAST(FixedString AS String)`.

SELECT 'keep';
SET cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT hex(arrayStringConcat([toFixedString('a', 2)]));
SELECT hex(arrayStringConcat([toFixedString('a', 2), toFixedString('bc', 2)], '|'));
SELECT hex(arrayStringConcat(arrayMap(x -> toFixedString(toString(x), 2), range(number)), '|')) FROM numbers(3);
SELECT hex(arrayStringConcat([toFixedString('a', 2), NULL], '|'));

SELECT 'strip';
SET cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT hex(arrayStringConcat([toFixedString('a', 2)]));
SELECT hex(arrayStringConcat([toFixedString('a', 2), toFixedString('bc', 2)], '|'));
SELECT hex(arrayStringConcat(arrayMap(x -> toFixedString(toString(x), 2), range(number)), '|')) FROM numbers(3);
SELECT hex(arrayStringConcat([toFixedString('a', 2), NULL], '|'));

SELECT 'compatibility';
SET compatibility = '26.9';
SELECT hex(arrayStringConcat([toFixedString('a', 2)]));
