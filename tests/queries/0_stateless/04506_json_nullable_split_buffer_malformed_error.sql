SET session_timezone = 'Etc/UTC';

-- A malformed "new ISODate(" near-miss with a tiny read buffer should throw a clean parse error, not crash.
SELECT * FROM format(JSONEachRow, 'id UInt8, ts Nullable(DateTime64(3))', '{"id": 1, "ts": new Foo(1)}')
    SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0; -- { serverError CANNOT_PARSE_DATETIME }
