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
