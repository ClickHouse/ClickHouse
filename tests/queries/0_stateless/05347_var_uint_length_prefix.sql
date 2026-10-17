-- `toVarUInt` encodes a number as an unsigned LEB128 `VarUInt`, `addVarUIntLengthPrefix` puts the length of a string in front of it as such a number, `removeVarUIntLengthPrefix` takes it off.

SELECT 'toVarUInt';
SELECT hex(toVarUInt(0)), hex(toVarUInt(127)), hex(toVarUInt(128)), hex(toVarUInt(300)), hex(toVarUInt(16383)), hex(toVarUInt(16384)), hex(toVarUInt(toUInt64(18446744073709551615)));
SELECT hex(toVarUInt(toUInt8(200))), hex(toVarUInt(toUInt16(60000))), hex(toVarUInt(toUInt32(4000000000)));
SELECT hex(toVarUInt(number)) FROM numbers(3);

SELECT 'addVarUIntLengthPrefix';
SELECT hex(addVarUIntLengthPrefix('abc')), hex(addVarUIntLengthPrefix('')), hex(addVarUIntLengthPrefix(toFixedString('ab', 4)));
SELECT hex(substring(addVarUIntLengthPrefix(repeat('a', 127)), 1, 2)), hex(substring(addVarUIntLengthPrefix(repeat('a', 128)), 1, 3)), hex(substring(addVarUIntLengthPrefix(repeat('a', 300)), 1, 3));
SELECT length(addVarUIntLengthPrefix(repeat('a', 127))), length(addVarUIntLengthPrefix(repeat('a', 128))), length(addVarUIntLengthPrefix(repeat('a', 16384)));

SELECT 'removeVarUIntLengthPrefix';
SELECT removeVarUIntLengthPrefix(unhex('03616263')), hex(removeVarUIntLengthPrefix(unhex('00'))), removeVarUIntLengthPrefix(toFixedString(unhex('0161'), 2));
SELECT 'a padded length is read as RowBinary reads it';
SELECT removeVarUIntLengthPrefix(unhex('810061')), removeVarUIntLengthPrefix(unhex('81800061'));

SELECT 'a round trip gives the same string';
SELECT countIf(removeVarUIntLengthPrefix(addVarUIntLengthPrefix(s)) = s), count() FROM (SELECT repeat(char(number % 256), number * 37 % 40000) AS s FROM numbers(200));
SELECT removeVarUIntLengthPrefix(addVarUIntLengthPrefix(CAST(NULL, 'Nullable(String)'))) IS NULL;
SELECT addVarUIntLengthPrefix(materialize('')) = unhex('00');

SELECT 'wrong lengths';
SELECT removeVarUIntLengthPrefix(''); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('80')); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('0261')); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('016162')); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('00ff')); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('FFFFFFFFFFFFFFFFFF02')); -- { serverError INCORRECT_DATA }
SELECT removeVarUIntLengthPrefix(unhex('FFFFFFFFFFFFFFFFFFFF01')); -- { serverError INCORRECT_DATA }

SELECT 'wrong argument types';
SELECT toVarUInt(-1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toVarUInt(toInt64(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toVarUInt('1'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT addVarUIntLengthPrefix(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT removeVarUIntLengthPrefix(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
