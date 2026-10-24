-- `if` keeps the zero padding of a `FixedString` branch converted to `String` regardless of
-- `cast_fixed_string_to_string_strip_trailing_zeros`, so the rewrites between `if` chains and `multiIf`
-- must not change the result. `groupConcat` over `FixedString` follows the setting.

SET optimize_if_chain_to_multiif = 1, optimize_multiif_to_if = 1;

SELECT 'keep';
SET cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT hex(if(number = 0, 'x', if(number = 1, toFixedString('a', 2), 'y'))) FROM numbers(2) WHERE number = 1;
SELECT hex(multiIf(number = 1, toFixedString('a', 2), 'y')) FROM numbers(2) WHERE number = 1;
SELECT hex(groupConcat('|')(toFixedString(toString(number), 2))) FROM numbers(3);

SELECT 'strip';
SET cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT hex(if(number = 0, 'x', if(number = 1, toFixedString('a', 2), 'y'))) FROM numbers(2) WHERE number = 1;
SELECT hex(multiIf(number = 1, toFixedString('a', 2), 'y')) FROM numbers(2) WHERE number = 1;
SELECT hex(groupConcat('|')(toFixedString(toString(number), 2))) FROM numbers(3);
SELECT hex(groupConcat('|')(toFixedString(toString(number), 2))) = hex(groupConcat('|')(toFixedString(toString(number), 2)::String)) FROM numbers(3);
