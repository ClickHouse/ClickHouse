-- Tests for reinterpret(<source string>, <array_of_fixed_type>)

SELECT 'Verify correct destination type is instantiated';
SELECT toTypeName(reinterpret(x'01010101', 'Array(Int32)'));
SELECT toTypeName(reinterpret(x'01010101', 'Array(UInt32)'));
SELECT toTypeName(reinterpret(x'01010101', 'Array(Float32)'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(Int64)'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(UInt64)'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(Float64)'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(FixedString(4))'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(Decimal)'));
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(Decimal64(4))'));

SELECT 'Unsupported destination type';
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(String)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toTypeName(reinterpret(x'0101010101010101', 'Array(Array(Int32))')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Verify few output values are correct';

SELECT reinterpret(x'01010101', 'Int32');
SELECT reinterpret(x'01010101', 'Array(Int32)');
SELECT reinterpret(x'0101010102020202', 'Array(Int32)');

SELECT reinterpret(x'3108b440', 'Float32');
SELECT reinterpret(x'3108d440', 'Float32');
SELECT reinterpret(x'3108b4403108d4403108b4403108d440', 'Array(Float32)');
SELECT reinterpret(repeat(x'3108b4403108d4403108b4403108d440', 10), 'Array(Float32)');
SELECT length(reinterpret(repeat(x'3108b4403108d4403108b4403108d440', 10), 'Array(Float32)'));

SELECT reinterpret('abab', 'Array(FixedString(4))');
SELECT length(reinterpret(repeat('abab', 100), 'Array(FixedString(4))')) = 100;

SELECT 'Input data of wrong length should throw error';
SELECT reinterpret('ababc', 'Array(FixedString(4))'); -- { serverError BAD_ARGUMENTS }
SELECT reinterpret(x'3108d4', 'Array(Float32)'); -- { serverError BAD_ARGUMENTS }
SELECT reinterpret(concat(repeat(x'3108b4403108d4403108b4403108d440', 10),  x'aa'), 'Array(Float32)'); -- { serverError BAD_ARGUMENTS }

SELECT 'Wrong source type should throw error';
SELECT reinterpret(95, 'Array(FixedString(4))'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Couple of tests with FixedString input';
DROP TABLE IF EXISTS tab1;
CREATE TABLE tab1 (id Int32, s FixedString(8)) Engine = Memory;
INSERT INTO tab1 VALUES (1, x'3108b4403108d440');
INSERT INTO tab1 VALUES (2, x'3108d4403108b440');

SELECT reinterpret(s, 'Array(Float32)') FROM tab1 ORDER BY id;

DROP TABLE tab1;
