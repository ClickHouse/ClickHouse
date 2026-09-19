SET session_timezone = 'Asia/Kolkata';

-- A duplicated 'Z' suffix is malformed and must be rejected, not silently accepted as UTC.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3, \'Asia/Kolkata\')', '{"ts": ISODate("2024-05-29T23:16:12.256ZZ")}')
    SETTINGS date_time_input_format = 'basic'; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
