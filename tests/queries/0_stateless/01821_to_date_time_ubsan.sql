SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

SELECT toDateTime('9223372036854775806', 7, 'Asia/Istanbul');
SELECT toDateTime('9223372036854775806', 8, 'Asia/Istanbul');
