SELECT 'The reference time point is 2023-05-31 23:30:00';
SELECT '───────────────────────────────';
SELECT 'Before the reference time point';
SELECT '───────────────────────────────';

WITH
    3600 AS delta,
    toDateTime('2023-05-31 23:30:00') AS dt_ref,
    now() AS dt_now, 
    dt_now - delta as dt_before,
    dateDiff('second', dt_ref, dt_now) AS time_shift,
    formatDateTime(dt_before, '%b %e %T') AS syslog_before
SELECT
    formatDateTime(dt_before - time_shift, '%b %e %T') AS syslog_arg,
    parseDateTimeBestEffort(syslog_before) - time_shift AS res,
    parseDateTimeBestEffort(syslog_before, 'US/Samoa') - time_shift AS res_sam,
    parseDateTimeBestEffort(syslog_before, 'Pacific/Auckland') - time_shift AS res_auc,
    parseDateTimeBestEffortOrNull(syslog_before) - time_shift AS res_null,
    parseDateTimeBestEffortOrNull(syslog_before, 'US/Samoa') - time_shift AS res_null_sam,
    parseDateTimeBestEffortOrNull(syslog_before, 'Pacific/Auckland') - time_shift AS res_null_auc,
    parseDateTimeBestEffortOrZero(syslog_before) - time_shift AS res_zero,
    parseDateTimeBestEffortOrZero(syslog_before, 'US/Samoa') - time_shift AS res_zero_sam,
    parseDateTimeBestEffortOrZero(syslog_before, 'Pacific/Auckland') - time_shift AS res_zero_auc,
    parseDateTimeBestEffortUS(syslog_before) - time_shift AS res_us,
    parseDateTimeBestEffortUS(syslog_before, 'US/Samoa') - time_shift AS res_us_sam,
    parseDateTimeBestEffortUS(syslog_before, 'Pacific/Auckland') - time_shift AS res_us_auc,
    parseDateTimeBestEffortUSOrNull(syslog_before) - time_shift AS res_us_null,
    parseDateTimeBestEffortUSOrNull(syslog_before, 'US/Samoa') - time_shift AS res_us_null_sam,
    parseDateTimeBestEffortUSOrNull(syslog_before, 'Pacific/Auckland') - time_shift AS res_us_null_auc,
    parseDateTimeBestEffortUSOrZero(syslog_before) - time_shift AS res_us_zero,
    parseDateTimeBestEffortUSOrZero(syslog_before, 'US/Samoa') - time_shift AS res_us_zero_sam,
    parseDateTimeBestEffortUSOrZero(syslog_before, 'Pacific/Auckland') - time_shift AS res_us_zero_auc,
    parseDateTime64BestEffort(syslog_before) - time_shift AS res64,
    parseDateTime64BestEffort(syslog_before, 3, 'US/Samoa') - time_shift AS res64_sam,
    parseDateTime64BestEffort(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_auc,
    parseDateTime64BestEffortOrNull(syslog_before) - time_shift AS res64_null,
    parseDateTime64BestEffortOrNull(syslog_before, 3, 'US/Samoa') - time_shift AS res64_null_sam,
    parseDateTime64BestEffortOrNull(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_null_auc,
    parseDateTime64BestEffortOrZero(syslog_before) - time_shift AS res64_zero,
    parseDateTime64BestEffortOrZero(syslog_before, 3, 'US/Samoa') - time_shift AS res64_zero_sam,
    parseDateTime64BestEffortOrZero(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_zero_auc,
    parseDateTime64BestEffortUS(syslog_before) - time_shift AS res64_us,
    parseDateTime64BestEffortUS(syslog_before, 3, 'US/Samoa') - time_shift AS res64_us_sam,
    parseDateTime64BestEffortUS(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_us_auc,
    parseDateTime64BestEffortUSOrNull(syslog_before) - time_shift AS res64_us_null,
    parseDateTime64BestEffortUSOrNull(syslog_before, 3, 'US/Samoa') - time_shift AS res64_us_null_sam,
    parseDateTime64BestEffortUSOrNull(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_us_null_auc,
    parseDateTime64BestEffortUSOrZero(syslog_before) - time_shift AS res64_us_zero,
    parseDateTime64BestEffortUSOrZero(syslog_before, 3, 'US/Samoa') - time_shift AS res64_us_zero_sam,
    parseDateTime64BestEffortUSOrZero(syslog_before, 3, 'Pacific/Auckland') - time_shift AS res64_us_zero_auc
FORMAT Vertical;

SELECT '──────────────────────────────';
SELECT 'After the reference time point';
SELECT '──────────────────────────────';

WITH
    3600 AS delta,
    toDateTime('2023-05-31 23:30:00') AS dt_ref,
    now() AS dt_now, 
    dt_now + delta as dt_after,
    dateDiff('second', dt_ref, dt_now) AS time_shift,
    formatDateTime(dt_after, '%b %e %T') AS syslog_after
SELECT
    formatDateTime(dt_after - time_shift, '%b %e %T') AS syslog_arg,
    parseDateTimeBestEffort(syslog_after) - time_shift AS res,
    parseDateTimeBestEffort(syslog_after, 'US/Samoa') - time_shift AS res_sam,
    parseDateTimeBestEffort(syslog_after, 'Pacific/Auckland') - time_shift AS res_auc,
    parseDateTimeBestEffortOrNull(syslog_after) - time_shift AS res_null,
    parseDateTimeBestEffortOrNull(syslog_after, 'US/Samoa') - time_shift AS res_null_sam,
    parseDateTimeBestEffortOrNull(syslog_after, 'Pacific/Auckland') - time_shift AS res_null_auc,
    parseDateTimeBestEffortOrZero(syslog_after) - time_shift AS res_zero,
    parseDateTimeBestEffortOrZero(syslog_after, 'US/Samoa') - time_shift AS res_zero_sam,
    parseDateTimeBestEffortOrZero(syslog_after, 'Pacific/Auckland') - time_shift AS res_zero_auc,
    parseDateTimeBestEffortUS(syslog_after) - time_shift AS res_us,
    parseDateTimeBestEffortUS(syslog_after, 'US/Samoa') - time_shift AS res_us_sam,
    parseDateTimeBestEffortUS(syslog_after, 'Pacific/Auckland') - time_shift AS res_us_auc,
    parseDateTimeBestEffortUSOrNull(syslog_after) - time_shift AS res_us_null,
    parseDateTimeBestEffortUSOrNull(syslog_after, 'US/Samoa') - time_shift AS res_us_null_sam,
    parseDateTimeBestEffortUSOrNull(syslog_after, 'Pacific/Auckland') - time_shift AS res_us_null_auc,
    parseDateTimeBestEffortUSOrZero(syslog_after) - time_shift AS res_us_zero,
    parseDateTimeBestEffortUSOrZero(syslog_after, 'US/Samoa') - time_shift AS res_us_zero_sam,
    parseDateTimeBestEffortUSOrZero(syslog_after, 'Pacific/Auckland') - time_shift AS res_us_zero_auc,
    parseDateTime64BestEffort(syslog_after) - time_shift AS res64,
    parseDateTime64BestEffort(syslog_after, 3, 'US/Samoa') - time_shift AS res64_sam,
    parseDateTime64BestEffort(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_auc,
    parseDateTime64BestEffortOrNull(syslog_after) - time_shift AS res64_null,
    parseDateTime64BestEffortOrNull(syslog_after, 3, 'US/Samoa') - time_shift AS res64_null_sam,
    parseDateTime64BestEffortOrNull(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_null_auc,
    parseDateTime64BestEffortOrZero(syslog_after) - time_shift AS res64_zero,
    parseDateTime64BestEffortOrZero(syslog_after, 3, 'US/Samoa') - time_shift AS res64_zero_sam,
    parseDateTime64BestEffortOrZero(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_zero_auc,
    parseDateTime64BestEffortUS(syslog_after) - time_shift AS res64_us,
    parseDateTime64BestEffortUS(syslog_after, 3, 'US/Samoa') - time_shift AS res64_us_sam,
    parseDateTime64BestEffortUS(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_us_auc,
    parseDateTime64BestEffortUSOrNull(syslog_after) - time_shift AS res64_us_null,
    parseDateTime64BestEffortUSOrNull(syslog_after, 3, 'US/Samoa') - time_shift AS res64_us_null_sam,
    parseDateTime64BestEffortUSOrNull(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_us_null_auc,
    parseDateTime64BestEffortUSOrZero(syslog_after) - time_shift AS res64_us_zero,
    parseDateTime64BestEffortUSOrZero(syslog_after, 3, 'US/Samoa') - time_shift AS res64_us_zero_sam,
    parseDateTime64BestEffortUSOrZero(syslog_after, 3, 'Pacific/Auckland') - time_shift AS res64_us_zero_auc
FORMAT Vertical;
