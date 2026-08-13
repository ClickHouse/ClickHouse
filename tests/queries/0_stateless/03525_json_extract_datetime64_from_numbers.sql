set session_timezone='UTC';

-- Since 26.8, an unquoted number for a DateTime64 column is a Unix timestamp in seconds, consistent
-- with the Values format, CAST and toDateTime64. https://github.com/ClickHouse/ClickHouse/pull/108091
select JSONExtract('{"utc" : 1747771112}', 'utc', 'DateTime64(3)');
select '{"utc" : 1747771112}'::JSON(utc DateTime64);

-- The pre-26.8 behavior (reading the number as the raw scaled value / ticks) is available via the
-- input_format_read_datetime_number_as_raw_value compatibility setting.
set input_format_read_datetime_number_as_raw_value = 1;
select JSONExtract('{"utc" : 1747771112221}', 'utc', 'DateTime64(3)');
select JSONExtract('{"utc" : -1747771112221}', 'utc', 'DateTime64(3)');
select '{"utc" : 1747771112221}'::JSON(utc DateTime64);
select '{"utc" : -1747771112221}'::JSON(utc DateTime64);
