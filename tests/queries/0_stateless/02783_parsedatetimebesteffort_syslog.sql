SET session_timezone = 'UTC';
SET formatdatetime_e_with_space_padding = 1;

SELECT 'The reference time point is 2023-06-30 23:59:30';
SELECT '───────────────────────────────────────────────';
SELECT 'The argument is before the reference time point';
SELECT '───────────────────────────────────────────────';

WITH
    toDateTime('2023-06-30 23:59:30') AS dt_ref,
    now() AS dt_now,
    date_sub(DAY, 1, dt_now) as dt_before,
    dateDiff(SECOND, dt_ref, dt_now) AS time_shift,
    formatDateTime(dt_before, '%b %e %T') AS syslog_before
SELECT
    formatDateTime(dt_before - time_shift, '%b %e %T') AS syslog_arg,
    parseDateTimeBestEffort(syslog_before) - time_shift AS res,
    parseDateTimeBestEffortOrNull(syslog_before) - time_shift AS res_null,
    parseDateTimeBestEffortOrZero(syslog_before) - time_shift AS res_zero,
    parseDateTimeBestEffortUS(syslog_before) - time_shift AS res_us,
    parseDateTimeBestEffortUSOrNull(syslog_before) - time_shift AS res_us_null,
    parseDateTimeBestEffortUSOrZero(syslog_before) - time_shift AS res_us_zero,
    parseDateTime64BestEffort(syslog_before) - time_shift AS res64,
    parseDateTime64BestEffortOrNull(syslog_before) - time_shift AS res64_null,
    parseDateTime64BestEffortOrZero(syslog_before) - time_shift AS res64_zero,
    parseDateTime64BestEffortUS(syslog_before) - time_shift AS res64_us,
    parseDateTime64BestEffortUSOrNull(syslog_before) - time_shift AS res64_us_null,
    parseDateTime64BestEffortUSOrZero(syslog_before) - time_shift AS res64_us_zero
FORMAT Vertical;

SELECT '──────────────────────────────────────────────';
SELECT 'The argument is after the reference time point';
SELECT '──────────────────────────────────────────────';

WITH
    toDateTime('2023-06-30 23:59:30') AS dt_ref,
    now() AS dt_now,
    date_add(DAY, 1, dt_now) as dt_after,
    dateDiff(SECOND, dt_ref, dt_now) AS time_shift,
    formatDateTime(dt_after, '%b %e %T') AS syslog_after
SELECT
    formatDateTime(dt_after - time_shift, '%b %e %T') AS syslog_arg,
    parseDateTimeBestEffort(syslog_after) - time_shift AS res,
    parseDateTimeBestEffortOrNull(syslog_after) - time_shift AS res_null,
    parseDateTimeBestEffortOrZero(syslog_after) - time_shift AS res_zero,
    parseDateTimeBestEffortUS(syslog_after) - time_shift AS res_us,
    parseDateTimeBestEffortUSOrNull(syslog_after) - time_shift AS res_us_null,
    parseDateTimeBestEffortUSOrZero(syslog_after) - time_shift AS res_us_zero,
    parseDateTime64BestEffort(syslog_after) - time_shift AS res64,
    parseDateTime64BestEffortOrNull(syslog_after) - time_shift AS res64_null,
    parseDateTime64BestEffortOrZero(syslog_after) - time_shift AS res64_zero,
    parseDateTime64BestEffortUS(syslog_after) - time_shift AS res64_us,
    parseDateTime64BestEffortUSOrNull(syslog_after) - time_shift AS res64_us_null,
    parseDateTime64BestEffortUSOrZero(syslog_after) - time_shift AS res64_us_zero
FORMAT Vertical;
