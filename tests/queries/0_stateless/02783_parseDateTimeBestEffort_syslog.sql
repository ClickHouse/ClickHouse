SELECT 'parseDateTimeBestEffort';

WITH
    now() AS ts_now,
    '2023-06-07 04:55:30' AS ref_point,
    dateDiff('second', toDateTime(ref_point), ts_now) AS impedimenta,
    formatDateTime(ts_around, '%b %e %T') AS dt_curr
SELECT
    formatDateTime(ts_around - impedimenta, '%b %e %H:%i:%s') AS dt_ref,
    parseDateTimeBestEffort(dt_curr) - impedimenta AS res,
    parseDateTimeBestEffort(dt_curr, 'US/Samoa') - impedimenta AS res_sam,
    parseDateTimeBestEffort(dt_curr, 'Pacific/Auckland') - impedimenta AS res_auc,
    parseDateTimeBestEffortOrNull(dt_curr) - impedimenta AS res_null,
    parseDateTimeBestEffortOrNull(dt_curr, 'US/Samoa') - impedimenta AS res_null_sam,
    parseDateTimeBestEffortOrNull(dt_curr, 'Pacific/Auckland') - impedimenta AS res_null_auc,
    parseDateTimeBestEffortOrZero(dt_curr) - impedimenta AS res_zero,
    parseDateTimeBestEffortOrZero(dt_curr, 'US/Samoa') - impedimenta AS res_zero_sam,
    parseDateTimeBestEffortOrZero(dt_curr, 'Pacific/Auckland') - impedimenta AS res_zero_auc
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around)
FORMAT PrettySpaceNoEscapes;

SELECT 'parseDateTimeBestEffortUS';

WITH
    now() AS ts_now,
    '2023-06-07 04:55:30' AS ref_point,
    dateDiff('second', toDateTime(ref_point), ts_now) AS impedimenta,
    formatDateTime(ts_around, '%b %e %T') AS dt_curr
SELECT
    formatDateTime(ts_around - impedimenta, '%b %e %H:%i:%s') AS dt_ref,
    parseDateTimeBestEffortUS(dt_curr) - impedimenta AS res,
    parseDateTimeBestEffortUS(dt_curr, 'US/Samoa') - impedimenta AS res_sam,
    parseDateTimeBestEffortUS(dt_curr, 'Pacific/Auckland') - impedimenta AS res_auc,
    parseDateTimeBestEffortUSOrNull(dt_curr) - impedimenta AS res_null,
    parseDateTimeBestEffortUSOrNull(dt_curr, 'US/Samoa') - impedimenta AS res_null_sam,
    parseDateTimeBestEffortUSOrNull(dt_curr, 'Pacific/Auckland') - impedimenta AS res_null_auc,
    parseDateTimeBestEffortUSOrZero(dt_curr) - impedimenta AS res_zero,
    parseDateTimeBestEffortUSOrZero(dt_curr, 'US/Samoa') - impedimenta AS res_zero_sam,
    parseDateTimeBestEffortUSOrZero(dt_curr, 'Pacific/Auckland') - impedimenta AS res_zero_auc
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around)
FORMAT PrettySpaceNoEscapes;

SELECT 'parseDateTime64BestEffort';

WITH
    now() AS ts_now,
    '2023-06-07 04:55:30' AS ref_point,
    dateDiff('second', toDateTime(ref_point), ts_now) AS impedimenta,
    formatDateTime(ts_around, '%b %e %T') AS dt_curr
SELECT
    formatDateTime(ts_around - impedimenta, '%b %e %H:%i:%s') AS dt_ref,
    parseDateTime64BestEffort(dt_curr) - impedimenta AS res,
    parseDateTime64BestEffort(dt_curr, 3, 'US/Samoa') - impedimenta AS res_sam,
    parseDateTime64BestEffort(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_auc,
    parseDateTime64BestEffortOrNull(dt_curr) - impedimenta AS res_null,
    parseDateTime64BestEffortOrNull(dt_curr, 3, 'US/Samoa') - impedimenta AS res_null_sam,
    parseDateTime64BestEffortOrNull(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_null_auc,
    parseDateTime64BestEffortOrZero(dt_curr) - impedimenta AS res_zero,
    parseDateTime64BestEffortOrZero(dt_curr, 3, 'US/Samoa') - impedimenta AS res_zero_sam,
    parseDateTime64BestEffortOrZero(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_zero_auc
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around)
FORMAT PrettySpaceNoEscapes;

SELECT 'parseDateTime64BestEffortUS';

WITH
    now() AS ts_now,
    '2023-06-07 04:55:30' AS ref_point,
    dateDiff('second', toDateTime(ref_point), ts_now) AS impedimenta,
    formatDateTime(ts_around, '%b %e %T') AS dt_curr
SELECT
    formatDateTime(ts_around - impedimenta, '%b %e %H:%i:%s') AS dt_ref,
    parseDateTime64BestEffortUS(dt_curr) - impedimenta AS res,
    parseDateTime64BestEffortUS(dt_curr, 3, 'US/Samoa') - impedimenta AS res_sam,
    parseDateTime64BestEffortUS(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_auc,
    parseDateTime64BestEffortUSOrNull(dt_curr) - impedimenta AS res_null,
    parseDateTime64BestEffortUSOrNull(dt_curr, 3, 'US/Samoa') - impedimenta AS res_null_sam,
    parseDateTime64BestEffortUSOrNull(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_null_auc,
    parseDateTime64BestEffortUSOrZero(dt_curr) - impedimenta AS res_zero,
    parseDateTime64BestEffortUSOrZero(dt_curr, 3, 'US/Samoa') - impedimenta AS res_zero_sam,
    parseDateTime64BestEffortUSOrZero(dt_curr, 3, 'Pacific/Auckland') - impedimenta AS res_zero_auc
FROM (SELECT arrayJoin([ts_now - 30, ts_now + 30]) AS ts_around)
FORMAT PrettySpaceNoEscapes;
