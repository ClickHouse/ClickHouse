SET session_timezone = 'Etc/UTC';

DROP TABLE IF EXISTS test_nullable_isodate_split;

CREATE TABLE test_nullable_isodate_split (id UInt8, ts Nullable(DateTime64(3))) ENGINE = Memory;

-- A malformed "new ISODate(" near-miss with a tiny read buffer should throw a clean parse error, not crash.
INSERT INTO test_nullable_isodate_split SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0 FORMAT JSONEachRow {"id": 1, "ts": new Foo(1)}; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

DROP TABLE test_nullable_isodate_split;
