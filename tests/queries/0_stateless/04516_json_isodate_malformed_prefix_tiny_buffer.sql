-- Same as 04515 but with a tiny read buffer, so the ISODate prefix probe's checkpoint/rollback
-- (PeekableReadBuffer) is exercised across a buffer boundary rather than within a single chunk.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate123}')
    SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0; -- { serverError CANNOT_PARSE_DATETIME }
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": new ISODate123}')
    SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0; -- { serverError CANNOT_PARSE_DATETIME }
-- Valid ISODate/new ISODate values must still parse correctly under the same tiny buffer.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": ISODate("2024-05-29T23:16:12.256")}')
    SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0;
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3)', '{"ts": new ISODate("2024-05-29T23:16:12.256")}')
    SETTINGS max_read_buffer_size = 1, input_format_parallel_parsing = 0;
