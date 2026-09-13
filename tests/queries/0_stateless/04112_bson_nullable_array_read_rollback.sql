-- Regression test for STID 2097-3451:
-- When `BSONEachRowRowInputFormat` parses a value into `Nullable(T)` it used to
-- push to the null map before recursing to read the nested value. If the nested
-- read threw (e.g. incompatible BSON type for the target column), the null map
-- was left one entry ahead of the nested column. A subsequent rollback via
-- `SerializationArray::readArraySafe` would then hit
-- `ColumnNullable::popBack` and trip a `LOGICAL_ERROR` assertion
-- ("Cannot pop N rows from X: there are only M rows") that aborts the server
-- in debug / sanitizer builds.
--
-- The fix reorders the operations: read the nested value first, and only
-- advance the null map once the read has completed successfully.

SET input_format_parallel_parsing = 1;

-- BSON document: { a: [ "foo" ] } — the array element's type (STRING) is
-- incompatible with the target column type Array(Nullable(Int64)). The parser
-- must report a clean `ILLEGAL_COLUMN` exception and not abort with a
-- `LOGICAL_ERROR`.
--
-- Encoding (24 bytes):
--   18 00 00 00                outer document size = 24
--   04                         ARRAY type
--   61 00                      key "a\0"
--   10 00 00 00                inner document size = 16
--   02                         STRING type
--   30 00                      key "0\0"
--   04 00 00 00                string length (including trailing \0) = 4
--   66 6f 6f 00                value "foo\0"
--   00                         inner document terminator
--   00                         outer document terminator
SELECT * FROM format(BSONEachRow, 'a Array(Nullable(Int64))',
  x'180000000461001000000002300004000000666f6f000000'); -- { serverError ILLEGAL_COLUMN }

-- The same trigger at the top level: Nullable(Int64) with a STRING value.
--
-- Encoding (16 bytes):
--   10 00 00 00                outer document size = 16
--   02                         STRING type
--   61 00                      key "a\0"
--   04 00 00 00                string length = 4
--   66 6f 6f 00                value "foo\0"
--   00                         outer document terminator
SELECT * FROM format(BSONEachRow, 'a Nullable(Int64)',
  x'1000000002610004000000666f6f0000'); -- { serverError ILLEGAL_COLUMN }

-- Sanity check: a well-formed BSON document with a nullable array still works.
-- Encoding of { a: [1, 2, 3] } as Array(Nullable(Int64)) in BSON (46 bytes):
--   2e 00 00 00                outer document size = 46
--   04                         ARRAY type
--   61 00                      key "a\0"
--   26 00 00 00                inner document size = 38
--   12 30 00 01 00 00 00 00 00 00 00   INT64 "0" = 1
--   12 31 00 02 00 00 00 00 00 00 00   INT64 "1" = 2
--   12 32 00 03 00 00 00 00 00 00 00   INT64 "2" = 3
--   00                         inner terminator
--   00                         outer terminator
SELECT * FROM format(BSONEachRow, 'a Array(Nullable(Int64))',
  x'2e000000046100260000001230000100000000000000123100020000000000000012320003000000000000000000');

SET enable_analyzer = 1; -- formatRow(... AS val) needs the analyzer: the old path drops the alias, the BSON field is skipped as unknown, and the error assertions never fire

-- Coverage for src/Formats/BSONTypes.cpp: getBSONType, getBSONBinarySubtype,
-- getBSONTypeName, getBSONBinarySubtypeName — called in error paths of
-- BSONEachRowRowInputFormat but never exercised by existing CI tests.

-- getBSONType() false branch: unknown type byte 0x20 outside valid range
-- BSON doc (10 bytes): \x0A\x00\x00\x00 | type=0x20 | 'val\0' | term=\x00
SELECT * FROM format('BSONEachRow', 'val String',
    unhex('0A000000' || '20' || '76616C00' || '00')); -- { serverError UNKNOWN_TYPE }

-- getBSONBinarySubtype() false branch: subtype 0x08 (> 0x07 threshold)
-- BSON doc (19 bytes): size | type=0x05 (Binary) | 'val\0' | len=4 | subtype=0x08 | data | term
SELECT * FROM format('BSONEachRow', 'val String',
    unhex('13000000' || '05' || '76616C00' || '04000000' || '08' || '00010203' || '00')); -- { serverError UNKNOWN_TYPE }

-- getBSONTypeName(DOUBLE): Float64 written as BSON Double, read into IPv6 column
SELECT * FROM format('BSONEachRow', 'val IPv6',
    formatRow('BSONEachRow', 3.14::Float64 AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONTypeName(BOOL): Bool written as BSON Bool, read into IPv6 column
SELECT * FROM format('BSONEachRow', 'val IPv6',
    formatRow('BSONEachRow', true AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONTypeName(INT64): Int64 written as BSON Int64, read into IPv6 column
SELECT * FROM format('BSONEachRow', 'val IPv6',
    formatRow('BSONEachRow', 42::Int64 AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONTypeName(ARRAY): Array written as BSON Array, read into String column
SELECT * FROM format('BSONEachRow', 'val String',
    formatRow('BSONEachRow', [1, 2, 3]::Array(Int32) AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONTypeName(DOCUMENT): Map written as BSON Document, read into String column
SELECT * FROM format('BSONEachRow', 'val String',
    formatRow('BSONEachRow', map('a', 1)::Map(String, Int32) AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONTypeName(DATETIME): DateTime64 written as BSON Datetime, read into String column
SELECT * FROM format('BSONEachRow', 'val String',
    formatRow('BSONEachRow', toDateTime64('2024-01-01 00:00:00', 3) AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONBinarySubtypeName(UUID) into String: UUID written as BSON Binary/UUID subtype
SELECT * FROM format('BSONEachRow', 'val String',
    formatRow('BSONEachRow', toUUID('550e8400-e29b-41d4-a716-446655440000') AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONBinarySubtypeName(BINARY) into UUID: FixedString(8) written as BSON Binary/Binary subtype
SELECT * FROM format('BSONEachRow', 'val UUID',
    formatRow('BSONEachRow', toFixedString('abcdefgh', 8) AS val)); -- { serverError ILLEGAL_COLUMN }

-- getBSONBinarySubtypeName(UUID) into IPv6: UUID written as BSON Binary/UUID subtype
SELECT * FROM format('BSONEachRow', 'val IPv6',
    formatRow('BSONEachRow', toUUID('550e8400-e29b-41d4-a716-446655440000') AS val)); -- { serverError ILLEGAL_COLUMN }
