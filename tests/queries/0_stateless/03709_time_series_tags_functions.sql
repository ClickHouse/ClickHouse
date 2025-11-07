SELECT '---- timeSeriesTagsToGroup() -> timeSeriesGroupToTags()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group1,
       timeSeriesGroupToTags(group1),
       timeSeriesTagsToGroup([], '__name__', 'http_failures') AS group2,
       timeSeriesTagsToGroup([], '__name__', 'http_failures') AS same_group2,
       throwIf(same_group2 != group2),
       timeSeriesGroupToTags(group2),
       timeSeriesTagsToGroup([]) AS empty_group,
       timeSeriesGroupToTags(toUInt64(0));

SELECT '---- timeSeriesStoreTags() -> timeSeriesIdToGroup(), timeSeriesIdToTags()';
SELECT 8374283493092 AS id,
       timeSeriesStoreTags(id, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS same_id,
       throwIf(same_id != id),
       timeSeriesIdToGroup(same_id) AS group,
       timeSeriesIdToTags(same_id),
       timeSeriesGroupToTags(group);

SELECT '---- timeSeriesExtractTag()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesExtractTag(group, '__name__'),
       timeSeriesExtractTag(group, 'env'),
       timeSeriesExtractTag(group, 'instance');

SELECT '---- timeSeriesCopyTags()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS dest_group,
       timeSeriesTagsToGroup([('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS src_group,
       timeSeriesCopyTags(dest_group, src_group, ['__name__', 'code', 'env']) AS result_group,
       timeSeriesGroupToTags(result_group);

SELECT '---- timeSeriesRemoveTag()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group_of_3,
       timeSeriesRemoveTag(group_of_3, '__name__') AS group_of_2,
       timeSeriesGroupToTags(group_of_2),
       timeSeriesRemoveTag(group_of_2, 'env') AS group_of_1,
       timeSeriesGroupToTags(group_of_1),
       timeSeriesRemoveTag(group_of_1, 'region') AS empty_group,
       timeSeriesGroupToTags(empty_group);

SELECT '---- timeSeriesRemoveTags()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group_of_3,
       timeSeriesRemoveTags(group_of_3, ['env', 'region']) AS group_of_1,
       timeSeriesGroupToTags(group_of_1),
       timeSeriesRemoveTags(group_of_1, ['__name__', 'nonexistent']) AS empty_group,
       timeSeriesGroupToTags(empty_group);

SELECT '---- timeSeriesRemoveAllTagsExcept()';
SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesRemoveAllTagsExcept(group, ['env']) AS result_group,
       timeSeriesGroupToTags(result_group);

SELECT '---- timeSeriesJoinTags';
SELECT timeSeriesTagsToGroup([('__name__', 'up'), ('job', 'api-server'), ('src1', 'a'), ('src2', 'b'), ('src3', 'c')]) AS group,
       timeSeriesJoinTags(group, 'foo', ',', ['src1', 'src2', 'src3']) AS result_group,
       timeSeriesGroupToTags(result_group);

SELECT '---- timeSeriesReplaceTag()';
SELECT timeSeriesTagsToGroup([('__name__', 'up'), ('job', 'api-server'), ('service', 'a:c')]) AS group,
       timeSeriesReplaceTag(group, 'foo', '$1', 'service', '(.*):.*') AS result_group,
       timeSeriesGroupToTags(result_group);
