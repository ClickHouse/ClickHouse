SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

select toStartOfInterval(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}

SELECT
    toDateTime64(-9223372036854775808, 1048575, toIntervalNanosecond(9223372036854775806), NULL),
    toStartOfInterval(toDateTime64(toIntervalNanosecond(toIntervalNanosecond(257), toDateTime64(toStartOfInterval(toDateTime64(NULL)))), '', 100), toIntervalNanosecond(toStartOfInterval(toDateTime64(toIntervalNanosecond(NULL), NULL)), -1)),
    toStartOfInterval(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}
