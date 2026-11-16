-- Tags: no-fasttest
-- no-fasttest: MsgPack format is not available in the fasttest environment

-- Regression test for an UndefinedBehaviorSanitizer report in the bundled msgpack-c.
-- An empty string is encoded as an empty FixStr (the single byte "\xa0"). While unpacking
-- it during schema inference, `create_object_visitor::visit_str` called
-- `std::memcpy(dst, nullptr, 0)`, which is undefined behavior because `std::memcpy` declares
-- both pointers as nonnull. See https://github.com/ClickHouse/msgpack-c

SET engine_file_truncate_on_insert = 1;
SET input_format_msgpack_number_of_columns = 1;

-- Write a single empty FixStr byte directly so the file contains "\xa0" rather than the
-- empty bin8 ("\xc4\x00") that ClickHouse emits for a String value.
INSERT INTO FUNCTION file(currentDatabase() || '_04365_empty_fixstr.msgpack', 'RawBLOB') SELECT unhex('a0');

DESCRIBE file(currentDatabase() || '_04365_empty_fixstr.msgpack', 'MsgPack');
SELECT 'data:';
SELECT * FROM file(currentDatabase() || '_04365_empty_fixstr.msgpack', 'MsgPack');
