-- Coverage for src/Formats/BSONTypes.cpp: getBSONType, getBSONBinarySubtype,
-- getBSONTypeName, getBSONBinarySubtypeName — all called in error paths of
-- BSONEachRowRowInputFormat but never exercised by existing CI tests.
-- Uses formatRow('BSONEachRow', ...) to produce binary BSON, then format() to
-- try reading it with an incompatible schema, plus unhex() for invalid-byte paths.
-- Requires the analyzer to correctly apply column aliases inside formatRow() so
-- that the generated BSON field name matches the target schema.
SET enable_analyzer = 1;

-- getBSONType() — false branch: unknown type byte 0x20 outside [0x01,0x13]/0xFF/0x7F
-- BSON doc (10 bytes): \x0A\x00\x00\x00 | type=0x20 | 'val\0' | term=\x00
SELECT * FROM format('BSONEachRow', 'val String',
    unhex('0A000000' || '20' || '76616C00' || '00')); -- { serverError UNKNOWN_TYPE }

-- getBSONBinarySubtype() — false branch: subtype 0x08 (> 0x07 threshold)
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
