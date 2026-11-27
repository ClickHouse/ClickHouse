select toStartOfInterval(toDateTime64('\0930-12-12 12:12:12.1234567', 3), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}

SELECT
    toDateTime64(-9223372036854775808, 1048575, toIntervalNanosecond(9223372036854775806), NULL),
    toStartOfInterval(toDateTime64(toIntervalNanosecond(toIntervalNanosecond(257), toDateTime64(toStartOfInterval(toDateTime64(NULL)))), '', 100), toIntervalNanosecond(toStartOfInterval(toDateTime64(toIntervalNanosecond(NULL), NULL)), -1)),
    toStartOfInterval(toDateTime64('\0930-12-12 12:12:12.1234567', 3), toIntervalNanosecond(1024)); -- {serverError DECIMAL_OVERFLOW}
