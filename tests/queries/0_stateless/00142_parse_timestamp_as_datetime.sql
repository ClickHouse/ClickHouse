SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

SELECT min(ts = toUInt32(toDateTime(toString(ts)))) FROM (SELECT 1000000000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
SELECT min(ts = toUInt32(toDateTime(toString(ts)))) FROM (SELECT 10000 + 1234 * number AS ts FROM system.numbers LIMIT 1000000);
