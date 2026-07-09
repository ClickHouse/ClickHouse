SET session_timezone = 'Asia/Kolkata';

-- 'basic' only special-cases a plain 'Z'; a numeric offset like '+05:30' must be rejected.
SELECT * FROM format(JSONEachRow, 'ts DateTime64(3, \'Asia/Kolkata\')', '{"ts": ISODate("2024-05-29T23:16:12.256+05:30")}')
    SETTINGS date_time_input_format = 'basic'; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
