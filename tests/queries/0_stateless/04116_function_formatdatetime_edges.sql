-- Edge cases and error paths for formatDateTime / formatDateTimeInJodaSyntax / fromUnixTimestamp.

SELECT '--- Error paths: formatDateTime ---';
-- Trailing percent
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), '%'); -- { serverError BAD_ARGUMENTS }
-- Argument validation
SELECT formatDateTime('string', '%F'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), '%F', 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), '%F', 'UTC', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), '%F', ''); -- { serverError BAD_ARGUMENTS,ILLEGAL_TYPE_OF_ARGUMENT }
-- Non-const format string
SELECT formatDateTime(toDateTime('2020-01-02 03:04:05', 'UTC'), materialize('%F')); -- { serverError ILLEGAL_COLUMN }

SELECT '--- Error paths: formatDateTimeInJodaSyntax ---';
-- Short time zone repetitions (<=3) should throw NOT_IMPLEMENTED
SELECT formatDateTimeInJodaSyntax(toDateTime('2020-01-02 03:04:05', 'UTC'), 'z'); -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTimeInJodaSyntax(toDateTime('2020-01-02 03:04:05', 'UTC'), 'zz'); -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTimeInJodaSyntax(toDateTime('2020-01-02 03:04:05', 'UTC'), 'zzz'); -- { serverError NOT_IMPLEMENTED }
-- 4+ repetitions are supported
SELECT formatDateTimeInJodaSyntax(toDateTime('2020-01-02 03:04:05', 'UTC'), 'zzzz');

SELECT '--- Error paths: fromUnixTimestamp ---';
SELECT fromUnixTimestamp(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT fromUnixTimestamp('not a number', '%F'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT fromUnixTimestamp(1, 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '--- Joda: all common format tokens ---';
-- Exercise many pre/post-conversion paths
WITH toDateTime('2024-05-07 08:09:10', 'UTC') AS d
SELECT
    formatDateTimeInJodaSyntax(d, 'G'),        -- Era
    formatDateTimeInJodaSyntax(d, 'C'),        -- Century
    formatDateTimeInJodaSyntax(d, 'Y'),        -- Year of era
    formatDateTimeInJodaSyntax(d, 'YYYY'),
    formatDateTimeInJodaSyntax(d, 'x'),        -- Week year
    formatDateTimeInJodaSyntax(d, 'xxxx'),
    formatDateTimeInJodaSyntax(d, 'w'),        -- Week of week year
    formatDateTimeInJodaSyntax(d, 'ww'),
    formatDateTimeInJodaSyntax(d, 'e'),        -- Day of week (number)
    formatDateTimeInJodaSyntax(d, 'E'),        -- Day of week (text)
    formatDateTimeInJodaSyntax(d, 'EEEE'),
    formatDateTimeInJodaSyntax(d, 'y'),        -- Year
    formatDateTimeInJodaSyntax(d, 'yyyy'),
    formatDateTimeInJodaSyntax(d, 'D'),        -- Day of year
    formatDateTimeInJodaSyntax(d, 'DDD'),
    formatDateTimeInJodaSyntax(d, 'M'),        -- Month of year
    formatDateTimeInJodaSyntax(d, 'MM'),
    formatDateTimeInJodaSyntax(d, 'MMM'),
    formatDateTimeInJodaSyntax(d, 'MMMM'),
    formatDateTimeInJodaSyntax(d, 'd'),        -- Day of month
    formatDateTimeInJodaSyntax(d, 'dd'),
    formatDateTimeInJodaSyntax(d, 'a'),        -- AM/PM
    formatDateTimeInJodaSyntax(d, 'K'),        -- Hour of half day (0-11)
    formatDateTimeInJodaSyntax(d, 'h'),        -- Clock hour of half day (1-12)
    formatDateTimeInJodaSyntax(d, 'H'),        -- Hour of day (0-23)
    formatDateTimeInJodaSyntax(d, 'k'),        -- Clock hour of day (1-24)
    formatDateTimeInJodaSyntax(d, 'm'),        -- Minute of hour
    formatDateTimeInJodaSyntax(d, 's'),        -- Second of minute
    formatDateTimeInJodaSyntax(d, 'S');        -- Millis of second

SELECT '--- Joda: literal escapes ---';
SELECT formatDateTimeInJodaSyntax(toDateTime('2024-05-07 08:09:10', 'UTC'), '''at'' HH:mm');
-- Two consecutive single quotes produce a literal '
SELECT formatDateTimeInJodaSyntax(toDateTime('2024-05-07 08:09:10', 'UTC'), 'HH '''' mm');
-- Non-alpha characters pass through
SELECT formatDateTimeInJodaSyntax(toDateTime('2024-05-07 08:09:10', 'UTC'), '~!@#');

SELECT '--- Joda: unsupported alpha triggers NOT_IMPLEMENTED ---';
SELECT formatDateTimeInJodaSyntax(toDateTime('2024-05-07 08:09:10', 'UTC'), 'q');   -- { serverError NOT_IMPLEMENTED }
SELECT formatDateTimeInJodaSyntax(toDateTime('2024-05-07 08:09:10', 'UTC'), 'Q');   -- { serverError NOT_IMPLEMENTED }

SELECT '--- MySQL: variable width formatters + containsOnlyFixedWidthMySQLFormatters branches ---';
-- Variable-width '%W'
SELECT formatDateTime(toDateTime('2024-05-07 08:09:10', 'UTC'), '[%W]');
-- '%M' acts as minute (fixed width) by default, or month name under setting
SELECT formatDateTime(toDateTime('2024-05-07 08:09:10', 'UTC'), '[%M]');
SELECT formatDateTime(toDateTime('2024-05-07 08:09:10', 'UTC'), '[%M]') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 1;
SELECT formatDateTime(toDateTime('2024-05-07 08:09:10', 'UTC'), '[%M]') SETTINGS formatdatetime_parsedatetime_m_is_month_name = 0;
-- '%c' / '%l' / '%k' - space padding depends on setting
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '|%c|%l|%k|');
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '|%c|%l|%k|') SETTINGS formatdatetime_format_without_leading_zeros = 1;
-- '%e' - space padding variant
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '[%e]');
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '[%e]') SETTINGS formatdatetime_e_with_space_padding = 0;
-- '%f' fraction
SELECT formatDateTime(toDateTime64('2024-01-05 03:04:05.123456', 6, 'UTC'), '%f');
SELECT formatDateTime(toDateTime64('2024-01-05 03:04:05.123456', 6, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '%f');
SELECT formatDateTime(toDateTime('2024-01-05 03:04:05', 'UTC'), '%f') SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT formatDateTime(toDateTime64('2024-01-05 03:04:05.12', 2, 'UTC'), '%f') SETTINGS formatdatetime_f_prints_scale_number_of_digits = 1;

SELECT '--- fromUnixTimestampInJodaSyntax ---';
SELECT fromUnixTimestampInJodaSyntax(1714000000::Int64, 'yyyy-MM-dd HH:mm:ss', 'UTC');
SELECT fromUnixTimestampInJodaSyntax(1714000000::Int64, 'yyyy-MM-dd', 'UTC');
