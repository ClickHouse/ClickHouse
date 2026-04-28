
SET enable_time_time64_type = 1;

-- Time64 CSV deserialization reject garbage after values
SELECT * FROM format(CSV, 'c1 Time64(3)', '12:30:00.123xyz') SETTINGS input_format_csv_use_default_on_bad_values=0; -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }

-- and with double quotes
SELECT * FROM format(CSV, 'c1 Time64(3)', '"12:30:00.123xyz"') SETTINGS input_format_csv_use_default_on_bad_values=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- valid unquoted Time64 CSV values work
SELECT * FROM format(CSV, 'c1 Time64(3)', '12:30:00.123');

-- valid quoted Time64 CSV values work
SELECT * FROM format(CSV, 'c1 Time64(3)', '"12:30:00.123"');
