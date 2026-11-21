-- The conversion of `FixedString` to `String` keeps the zero bytes that pad the value to its length,
-- unless `cast_fixed_string_to_string_strip_trailing_zeros` asks for the old behavior.

SELECT 'compatibility';
SELECT hex(CAST(toFixedString('a', 3) AS String)) SETTINGS compatibility = '26.9';

SELECT 'keep';
SET cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT hex(CAST(toFixedString('a', 3) AS String)), length(toString(toFixedString('a', 3))), length(toFixedString('a', 3)::String);
SELECT hex(CAST(toFixedString('a\0b', 4) AS String)), hex(CAST(toFixedString('', 2) AS String));
SELECT hex(CAST(toNullable(toFixedString('a', 2)) AS Nullable(String))), hex(CAST(toLowCardinality(toFixedString('a', 2)) AS String));
SELECT toFixedString('a', 2)::String = 'a', toFixedString('a', 2)::String = 'a\0';

SELECT 'strip';
SET cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT hex(CAST(toFixedString('a', 3) AS String)), length(toString(toFixedString('a', 3))), length(toFixedString('a', 3)::String);
SELECT hex(CAST(toFixedString('a\0b', 4) AS String)), hex(CAST(toFixedString('', 2) AS String));
SELECT hex(CAST(toNullable(toFixedString('a', 2)) AS Nullable(String))), hex(CAST(toLowCardinality(toFixedString('a', 2)) AS String));
SELECT toFixedString('a', 2)::String = 'a', toFixedString('a', 2)::String = 'a\0';
