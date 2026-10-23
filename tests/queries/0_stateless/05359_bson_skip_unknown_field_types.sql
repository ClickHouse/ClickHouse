-- Skipping an unknown field of every BSON type, as defined by https://bsonspec.org/spec.html.
-- Every document below is { "a": int32 42, "u": <one unknown field>, "z": string <type name> }.
-- "z" can only be decoded when the unknown field in front of it was skipped by exactly its
-- own length, so the pair (42, <type name>) is what proves the skip of that type correct.
SET input_format_skip_unknown_fields = 1;
-- The documents are a few dozen bytes, so the parallel parser would read each one as a single
-- chunk anyway; parse in one stream to keep the result independent of the chunk size.
SET input_format_parallel_parsing = 0;

-- 0x01 double
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'250000001061002a000000017500000000000000f83f027a0007000000646f75626c650000');
-- 0x02 string
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'240000001061002a00000002750003000000686900027a0007000000737472696e670000');
-- 0x03 document
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'240000001061002a0000000375000500000000027a0009000000646f63756d656e740000');
-- 0x04 array
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'210000001061002a0000000475000500000000027a000600000061727261790000');
-- 0x05 binary
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'240000001061002a0000000575000200000000aabb027a000700000062696e6172790000');
-- 0x06 undefined
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'200000001061002a000000067500027a000a000000756e646566696e65640000');
-- 0x07 object_id
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'2c0000001061002a0000000775000102030405060708090a0b0c027a000a0000006f626a6563745f69640000');
-- 0x08 bool
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'1c0000001061002a00000008750001027a0005000000626f6f6c0000');
-- 0x09 datetime
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'270000001061002a000000097500e803000000000000027a00090000006461746574696d650000');
-- 0x0A null
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'1b0000001061002a0000000a7500027a00050000006e756c6c0000');
-- 0x0B regexp
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'220000001061002a0000000b75006162006900027a00070000007265676578700000');
-- 0x0C db_pointer
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'340000001061002a0000000c7500030000006e73000102030405060708090a0b0c027a000b00000064625f706f696e7465720000');
-- 0x0D code
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'220000001061002a0000000d750003000000663100027a0005000000636f64650000');
-- 0x0E symbol
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'240000001061002a0000000e750003000000737900027a000700000073796d626f6c0000');
-- 0x0F code_w_scope
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'330000001061002a0000000f750010000000030000006631000500000000027a000d000000636f64655f775f73636f70650000');
-- 0x10 int32
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'200000001061002a00000010750007000000027a0006000000696e7433320000');
-- 0x11 timestamp
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'280000001061002a000000117500d007000000000000027a000a00000074696d657374616d700000');
-- 0x12 int64
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'240000001061002a000000127500b80b000000000000027a0006000000696e7436340000');
-- 0x13 decimal128
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'310000001061002a00000013750000000000000000000000000000000000027a000b000000646563696d616c3132380000');
-- 0xFF min_key
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'1e0000001061002a000000ff7500027a00080000006d696e5f6b65790000');
-- 0x7F max_key
SELECT a, z FROM format(BSONEachRow, 'a Int32, z String', x'1e0000001061002a0000007f7500027a00080000006d61785f6b65790000');

-- A nested document whose size field is smaller than an empty document, and a code_w_scope
-- whose size field is smaller than the size field itself: both are rejected, not skipped.
SELECT a FROM format(BSONEachRow, 'a Int32', x'0c0000000375000300000000'); -- { serverError INCORRECT_DATA }
SELECT a FROM format(BSONEachRow, 'a Int32', x'0c0000000f75000200000000'); -- { serverError INCORRECT_DATA }
