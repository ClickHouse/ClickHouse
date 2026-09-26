-- Tags: no-fasttest, no-random-settings
-- Regression test: absurdly large input_format_msgpack_number_of_columns should
-- not crash the server. Previously, vector::reserve() with a huge size caused
-- std::length_error (a std::logic_error subtype) which led to abort in
-- debug/sanitizer builds via getCurrentExceptionMessage().
-- Now the MsgPack reader validates the column count upfront.

-- Create a tiny file with arbitrary data (not valid MsgPack).
INSERT INTO FUNCTION file('04101_test.bin', 'RawBLOB') VALUES ('hello');

-- Values exceeding the 1M limit are rejected during schema inference.
SELECT * FROM file('04101_test.bin', 'MsgPack') SETTINGS input_format_msgpack_number_of_columns = 1152921504606846976; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM file('04101_test.bin', 'MsgPack') SETTINGS input_format_msgpack_number_of_columns = 999999999999999; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM file('04101_test.bin', 'MsgPack') SETTINGS input_format_msgpack_number_of_columns = 1000001; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
