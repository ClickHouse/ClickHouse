-- timeSeriesCopyTag and timeSeriesCopyTags return one row per input row when both group arguments are constant.

SELECT timeSeriesCopyTags(toUInt64(0), toUInt64(0), ['src']);
SELECT timeSeriesCopyTag(toUInt64(0), toUInt64(0), 'src');

SELECT timeSeriesCopyTags(toUInt64(0), toUInt64(0), ['src']) FROM numbers(3);
SELECT timeSeriesCopyTag(toUInt64(0), toUInt64(0), 'src') FROM numbers(3);

SELECT timeSeriesCopyTags(identity(toUInt64(0)), toUInt64(0), ['src']) FROM numbers(3);
SELECT timeSeriesCopyTags(toNullable(toUInt64(0)), toUInt64(0), ['src']) FROM numbers(3);
SELECT timeSeriesCopyTag(toUInt64(0), toLowCardinality(toUInt64(0)), 'src') FROM numbers(3);

WITH (SELECT timeSeriesTagsToGroup([('region', 'eu')], '__name__', 'dest_metric')) AS dest_group,
     (SELECT timeSeriesTagsToGroup([('code', '404')], '__name__', 'src_metric')) AS src_group
SELECT timeSeriesGroupToTags(timeSeriesCopyTags(dest_group, src_group, ['__name__', 'code'])),
       timeSeriesGroupToTags(timeSeriesCopyTag(dest_group, src_group, 'code'))
FROM numbers(3);

SELECT timeSeriesCopyTags(toUInt64(0), toUInt64(18446744073709551615), ['src']); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesCopyTag(toUInt64(0), toUInt64(18446744073709551615), 'src'); -- { serverError BAD_ARGUMENTS }
