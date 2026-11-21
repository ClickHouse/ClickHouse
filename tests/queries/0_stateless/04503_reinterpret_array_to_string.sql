-- https://github.com/ClickHouse/ClickHouse/issues/109379
-- reinterpret(<array of fixed size elements>, 'String') is the inverse of
-- reinterpret(<string>, 'Array(...)') and must be supported symmetrically.

SELECT 'Verify destination type is String';
SELECT toTypeName(reinterpret([toInt32(1)]::Array(Int32), 'String'));
SELECT toTypeName(reinterpretAsString([toInt32(1)]::Array(Int32)));

SELECT 'Verify output bytes are correct';
-- Single-byte elements and FixedString are byte-exact on every architecture (no byte-order ambiguity).
SELECT hex(reinterpret([toUInt8(1), toUInt8(2), toUInt8(255)]::Array(UInt8), 'String'));
SELECT hex(reinterpret(['ab', 'cd']::Array(FixedString(2)), 'String'));
-- Multi-byte elements are copied verbatim in native in-memory byte order, so their exact bytes are
-- endianness-dependent (e.g. [toUInt16(0x0102)] is 0201 on little-endian but 0102 on big-endian).
-- Verify the byte count (element size times element count) instead, which is architecture-independent;
-- the round-trip assertions below verify the element values on every architecture.
SELECT length(reinterpret([toUInt16(0x0102), toUInt16(0x0304)]::Array(UInt16), 'String'));
SELECT length(reinterpret([toInt32(1), toInt32(2), toInt32(3)]::Array(Int32), 'String'));
SELECT length(reinterpretAsString([toUInt16(0x0102), toUInt16(0x0304)]::Array(UInt16)));

SELECT 'Trailing zero bytes are copied verbatim, not trimmed';
-- The scalar reinterpretAsString trims trailing zero bytes (the String -> scalar path pads them
-- back), but the Array path must NOT trim: the String -> Array path requires an exact byte multiple
-- of the element size and does not pad, so trimming would break the round-trip and lose elements.
-- The byte count proves nothing is trimmed, independent of byte order (a single Int32 stays 4 bytes,
-- not the 1 byte the scalar path would leave).
SELECT length(reinterpret([toInt32(1)]::Array(Int32), 'String'));
SELECT hex(reinterpret([toUInt8(1), toUInt8(0)]::Array(UInt8), 'String'));
SELECT length(reinterpret([toUInt16(0x0100), toUInt16(0)]::Array(UInt16), 'String'));

SELECT 'Round-trips through String preserve every element, including trailing zeros';
SELECT reinterpret(reinterpret([toInt32(1)]::Array(Int32), 'String'), 'Array(Int32)');
SELECT reinterpret(reinterpret([toUInt8(1), toUInt8(0)]::Array(UInt8), 'String'), 'Array(UInt8)');
SELECT reinterpret(reinterpret([toInt32(1), toInt32(0), toInt32(2)]::Array(Int32), 'String'), 'Array(Int32)');

SELECT 'The exact scenario from the issue (round-trips through String)';
WITH [toBFloat16(1.5), toBFloat16(-2.25), toBFloat16(3.0)]::Array(BFloat16) AS target
SELECT reinterpret(reinterpret(target, 'String'), 'Array(BFloat16)');

SELECT 'Empty array reinterprets to an empty string';
SELECT length(reinterpret([]::Array(Int32), 'String'));

SELECT 'Array element type must be fixed length and contiguous';
SELECT reinterpret(['a', 'b']::Array(String), 'String'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT reinterpret([toInt32(1)]::Array(Nullable(Int32)), 'String'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT reinterpret([[toInt32(1)]]::Array(Array(Int32)), 'String'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'LowCardinality nested element is stripped and works';
SELECT reinterpret(reinterpret([toInt32(1), toInt32(2)]::Array(LowCardinality(Int32)), 'String'), 'Array(Int32)') SETTINGS allow_suspicious_low_cardinality_types = 1;
-- Non-numeric fixed-size element types (FixedString, UUID) work too. The framework recursively strips
-- the nested LowCardinality (recursiveRemoveLowCardinality) before return-type deduction, so the type
-- gate already sees the plain Array(FixedString(2)) / Array(UUID) and the contiguous-memory check passes.
-- The FixedString bytes are copied verbatim, so their hex is architecture-independent; the round-trips
-- verify the element values on every architecture.
SELECT hex(reinterpret(['ab', 'cd']::Array(LowCardinality(FixedString(2))), 'String')) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT reinterpret(reinterpret(['ab', 'cd']::Array(LowCardinality(FixedString(2))), 'String'), 'Array(FixedString(2))') SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT reinterpret(reinterpret([toUUID('00000000-0000-0000-0000-000000000001'), toUUID('00000000-0000-0000-0000-000000000002')]::Array(LowCardinality(UUID)), 'String'), 'Array(UUID)') SETTINGS allow_suspicious_low_cardinality_types = 1;

SELECT 'A few rows read from a table';
DROP TABLE IF EXISTS tab_04503;
CREATE TABLE tab_04503 (id Int32, arr Array(UInt8)) ENGINE = Memory;
INSERT INTO tab_04503 VALUES (1, [1, 2]), (2, [10, 20, 30]), (3, []);
SELECT id, hex(reinterpret(arr, 'String')) FROM tab_04503 ORDER BY id;
DROP TABLE tab_04503;
