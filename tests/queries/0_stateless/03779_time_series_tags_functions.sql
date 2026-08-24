SELECT 'timeSeriesTagsToGroup:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       timeSeriesGroupToTags(empty_group),
       empty_group
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group1,
           timeSeriesTagsToGroup([], '__name__', 'http_failures') AS group2,
           timeSeriesTagsToGroup([], '__name__', 'http_failures') AS same_group2,
           timeSeriesTagsToGroup([]) AS empty_group
);

SELECT '';
SELECT 'timeSeriesStoreTags:';

SELECT same_id1 == id1,
       same_id2 == id2,
       same_id3 == id3,
       same_id4 == id4,
       same_id5 == id5,
       same_id6 == id6,
       same_id7 == id7,
       group2 == group3,
       group4 == group5 AND group5 == group6,
       group7 == 0,
       timeSeriesIdToTags(id1) AS tags1,
       timeSeriesIdToTags(id2) AS tags2,
       timeSeriesIdToTags(id3) AS tags3,
       timeSeriesIdToTags(id4) AS tags4,
       timeSeriesIdToTags(id5) AS tags5,
       timeSeriesIdToTags(id6) AS tags6,
       timeSeriesIdToTags(id7) AS tags7,
       tags2 == tags3,
       tags4 == tags5 AND tags5 == tags6,
       empty(tags7),
       timeSeriesGroupToTags(group1) == tags1,
       timeSeriesGroupToTags(group2) == tags2,
       timeSeriesGroupToTags(group3) == tags3,
       timeSeriesGroupToTags(group4) == tags4,
       timeSeriesGroupToTags(group5) == tags5,
       timeSeriesGroupToTags(group6) == tags6,
       timeSeriesGroupToTags(group7) == tags7
FROM
(
    SELECT 1111111111111 AS id1,
           2222222222222 AS id2,
           3333333333333 AS id3,
           4444444444444 AS id4,
           5555555555555 AS id5,
           6666666666666 AS id6,
           7777777777777 AS id7,
           timeSeriesStoreTags(id1, [('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests') AS same_id1,
           timeSeriesStoreTags(id2, [], '__name__', 'http_request_by_region', 'region', 'us') AS same_id2,
           timeSeriesStoreTags(id3, NULL, 'region', 'us', '__name__', 'http_request_by_region') AS same_id3,
           timeSeriesStoreTags(id4, [], '__name__', 'http_requests_total') AS same_id4,
           timeSeriesStoreTags(id5, [], '__name__', 'http_requests_total', 'region', '') AS same_id5,
           timeSeriesStoreTags(id6, [], '__name__', 'http_requests_total', 'region', NULL) AS same_id6,
           timeSeriesStoreTags(id7, []) AS same_id7,
           timeSeriesIdToGroup(same_id1) AS group1,
           timeSeriesIdToGroup(same_id2) AS group2,
           timeSeriesIdToGroup(same_id3) AS group3,
           timeSeriesIdToGroup(same_id4) AS group4,
           timeSeriesIdToGroup(same_id5) AS group5,
           timeSeriesIdToGroup(same_id6) AS group6,
           timeSeriesIdToGroup(same_id7) AS group7
);

SELECT '';
SELECT 'timeSeriesExtractTag:';

SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS group,
       timeSeriesExtractTag(group, '__name__'),
       timeSeriesExtractTag(group, 'env'),
       timeSeriesExtractTag(group, 'instance');

SELECT '';
SELECT 'timeSeriesCopyTag:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       again_group2 == group2,
       timeSeriesGroupToTags(group3)
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS dest_group,
           timeSeriesTagsToGroup([('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS src_group,
           timeSeriesCopyTag(dest_group, src_group, '__name__') AS group1,
           timeSeriesCopyTag(group1, src_group, 'code') AS group2,
           timeSeriesCopyTag(group2, src_group, 'code') AS same_group2,
           timeSeriesCopyTag(group2, src_group, 'unknown') AS again_group2,
           timeSeriesCopyTag(group2, src_group, 'env') AS group3
);

SELECT '';
SELECT 'timeSeriesCopyTags:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       again_group2 == group2,
       timeSeriesGroupToTags(group3),
       same_group3 == group3,
       again_group3 == group3,
       same_dest_group == dest_group,
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS dest_group,
           timeSeriesTagsToGroup([('code', '404'), ('message', 'Page not found')], '__name__', 'http_codes') AS src_group,
           timeSeriesCopyTags(dest_group, src_group, ['__name__']) AS group1,
           timeSeriesCopyTags(group1, src_group, ['code']) AS group2,
           timeSeriesCopyTags(group2, src_group, ['code']) AS same_group2,
           timeSeriesCopyTags(group2, src_group, ['unknown']) AS again_group2,
           timeSeriesCopyTags(group2, src_group, ['env']) AS group3,
           timeSeriesCopyTags(dest_group, src_group, ['__name__', 'code', 'unknown', 'env']) AS same_group3,
           timeSeriesCopyTags(dest_group, src_group, ['code', 'code', 'env', '__name__', 'code']) AS again_group3,
           timeSeriesCopyTags(dest_group, src_group, []) AS same_dest_group
);

SELECT '';
SELECT 'timeSeriesRemoveTag:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       timeSeriesGroupToTags(empty_group),
       empty_group == 0
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS src_group,
           timeSeriesRemoveTag(src_group, '__name__') AS group1,
           timeSeriesRemoveTag(group1, 'env') AS group2,
           timeSeriesRemoveTag(group2, 'env') AS same_group2,
           timeSeriesRemoveTag(group2, 'region') AS empty_group
);

SELECT '';
SELECT 'timeSeriesRemoveTags:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       again_group2 == group2,
       timeSeriesGroupToTags(empty_group),
       empty_group == 0,
       same_src_group == src_group
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS src_group,
           timeSeriesRemoveTags(src_group, ['__name__']) AS group1,
           timeSeriesRemoveTags(group1, ['env']) AS group2,
           timeSeriesRemoveTags(group2, ['env']) AS same_group2,
           timeSeriesRemoveTags(src_group, ['__name__', 'env']) AS again_group2,
           timeSeriesRemoveTags(group2, ['region']) AS empty_group,
           timeSeriesRemoveTags(src_group, []) AS same_src_group
);

SELECT '';
SELECT 'timeSeriesRemoveAllTagsExcept:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       same_group2 == group2,
       again_group2 == group2,
       timeSeriesGroupToTags(empty_group),
       empty_group == 0,
       again_empty_group == 0
FROM
(
    SELECT timeSeriesTagsToGroup([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count') AS src_group,
           timeSeriesRemoveAllTagsExcept(src_group, ['region', 'env']) AS group1,
           timeSeriesRemoveAllTagsExcept(group1, ['region']) AS group2,
           timeSeriesRemoveAllTagsExcept(group2, ['region']) AS same_group2,
           timeSeriesRemoveAllTagsExcept(src_group, ['region']) AS again_group2,
           timeSeriesRemoveAllTagsExcept(group2, ['unknown']) AS empty_group,
           timeSeriesRemoveAllTagsExcept(group2, []) AS again_empty_group
);

SELECT '';
SELECT 'timeSeriesJoinTags:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       timeSeriesGroupToTags(group3),
       timeSeriesGroupToTags(group4),
       timeSeriesGroupToTags(group5),
       timeSeriesGroupToTags(group6),
       timeSeriesGroupToTags(group7),
       timeSeriesGroupToTags(group8)
FROM
(
    SELECT timeSeriesTagsToGroup([('__name__', 'up'), ('job', 'api-server'), ('src1', 'a'), ('src2', 'b'), ('src3', 'c')]) AS src_group,
           timeSeriesJoinTags(src_group, 'foo', ',', ['src1', 'src2', 'src3']) AS group1,
           timeSeriesJoinTags(src_group, 'foo', '', ['src1', 'src2', 'src3']) AS group2,
           timeSeriesJoinTags(src_group, 'foo', ' sep ', ['src1', 'src2', 'src3']) AS group3,
           timeSeriesJoinTags(src_group, 'foo', ',', []) AS group4,
           timeSeriesJoinTags(src_group, 'foo', ',', ['src1']) AS group5,
           timeSeriesJoinTags(src_group, 'foo', ',', ['src1', 'src2', 'src1', 'src4']) AS group6,
           timeSeriesJoinTags(src_group, 'job', ',', ['src1', 'src2', 'src3']) AS group7,
           timeSeriesJoinTags(src_group, 'job', '', []) AS group8
);

SELECT '';
SELECT 'timeSeriesReplaceTag:';

SELECT timeSeriesGroupToTags(group1),
       timeSeriesGroupToTags(group2),
       timeSeriesGroupToTags(group3),
       timeSeriesGroupToTags(group4),
       timeSeriesGroupToTags(group5),
       timeSeriesGroupToTags(group6),
       same_src_group == src_group,
       again_src_group == src_group,
       once_again_src_group == src_group
FROM
(
    SELECT timeSeriesTagsToGroup([('__name__', 'up'), ('job', 'api-server'), ('service', 'a:c')]) AS src_group,
           timeSeriesReplaceTag(src_group, 'foo', '$1', 'service', '(.*):.*') AS group1,
           timeSeriesReplaceTag(src_group, 'foo', '$0', 'service', '(.*):.*') AS group2,
           timeSeriesReplaceTag(src_group, 'foo', '$name', 'service', '(?P<name>.*):(?P<version>.*)') AS group3,
           timeSeriesReplaceTag(src_group, 'foo', 'begin ${name} sep ${version} end', 'service', '(?P<name>.*):(?P<version>.*)') AS group4,
           timeSeriesReplaceTag(src_group, 'service', '${2}:${1}', 'service', '(.*):(.*)') AS group5,
           timeSeriesReplaceTag(src_group, 'service', '', 'service', '(.*):(.*)') AS group6,
           timeSeriesReplaceTag(src_group, 'foo', '$0', 'service', 'unknown.*') AS same_src_group,
           timeSeriesReplaceTag(src_group, 'service', '$0', 'service', 'unknown.*') AS again_src_group,
           timeSeriesReplaceTag(src_group, 'foo', '$0', 'service', ':') AS once_again_src_group
);

SELECT '';
SELECT 'timeSeriesThrowDuplicateSeriesIf:';

SELECT timeSeriesThrowDuplicateSeriesIf(0, group)
FROM
(
    SELECT timeSeriesTagsToGroup([('__name__', 'up')]) AS group
);

SELECT timeSeriesThrowDuplicateSeriesIf(1, group)
FROM
(
    SELECT timeSeriesTagsToGroup([('__name__', 'up')]) AS group
);  -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
