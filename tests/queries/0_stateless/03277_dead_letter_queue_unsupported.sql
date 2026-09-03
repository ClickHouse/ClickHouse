SELECT 'qfilelog';
CREATE TABLE qfilelog (key UInt64, value UInt64) ENGINE = FileLog('/tmp/app.log', 'JSONEachRow')
SETTINGS handle_error_mode = 'dead_letter_queue'; -- { serverError BAD_ARGUMENTS }
