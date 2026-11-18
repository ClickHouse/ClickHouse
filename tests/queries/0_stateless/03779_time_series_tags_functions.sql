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

SELECT [('region', 'eu'), ('env', 'dev'), ('__name__', 'http_requests_count')] AS tags,
       timeSeriesExtractTag(tags, '__name__'),
       timeSeriesExtractTag(tags, 'env'),
       timeSeriesExtractTag(tags, 'instance'),
       isNotNull(timeSeriesExtractTag(tags, '__name__')),
       isNull(timeSeriesExtractTag(tags, 'instance'));

SELECT '';
SELECT 'timeSeriesExtractTag mixed rows:';

WITH
    (
        SELECT groupArray(tags)
        FROM
        (
            SELECT number,
                   if(number % 2 = 0,
                      [('env', 'dev')],
                      []) AS tags
            FROM numbers(4)
            ORDER BY number
        )
    ) AS tag_sets
SELECT countIf(isNull(value)),
       countIf(isNull(value) AND number % 2 = 1),
       countIf(isNotNull(value) AND number % 2 = 0)
FROM
(
    SELECT number,
           timeSeriesExtractTag(tag_sets[number + 1], 'env') AS value
    FROM numbers(4)
);

SELECT '';
SELECT 'timeSeriesGroupToSamplingKey:';

WITH
    (
        SELECT groupArray(timeSeriesGroupToSamplingKey(group))
        FROM
        (
            SELECT number,
                   multiIf(number % 3 = 0,
                           timeSeriesTagsToGroup([('env', 'dev')]),
                           number % 3 = 1,
                           timeSeriesTagsToGroup([('env', 'prod')]),
                           timeSeriesTagsToGroup([])) AS group
            FROM numbers(6)
            ORDER BY number
        )
    ) AS keys
SELECT [keys[1] = keys[4],
        keys[2] = keys[5],
        keys[3] = keys[6],
        keys[1] != keys[2],
        keys[1] != keys[3],
        keys[2] != keys[3]];

SELECT '';
SELECT 'timeSeriesCopyTag:';

SELECT tags1,
       tags2,
       same_tags2 == tags2,
       again_tags2 == tags2,
       tags3
FROM
(
    SELECT [('region', 'eu'), ('env', 'dev'), ('__name__', 'http_requests_count')] AS dest_tags,
           [('code', '404'), ('message', 'Page not found'), ('__name__', 'http_codes')] AS src_tags,
           timeSeriesCopyTag(dest_tags, src_tags, '__name__') AS tags1,
           timeSeriesCopyTag(tags1, src_tags, 'code') AS tags2,
           timeSeriesCopyTag(tags2, src_tags, 'code') AS same_tags2,
           timeSeriesCopyTag(tags2, src_tags, 'unknown') AS again_tags2,
           timeSeriesCopyTag(tags2, src_tags, 'env') AS tags3
);

SELECT '';
SELECT 'timeSeriesCopyTags:';

SELECT tags1,
       tags2,
       same_tags2 == tags2,
       again_tags2 == tags2,
       tags3,
       same_tags3 == tags3,
       again_tags3 == tags3,
       same_dest_tags == dest_tags,
FROM
(
    SELECT [('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')] AS dest_tags,
           [('__name__', 'http_codes'), ('code', '404'), ('message', 'Page not found')] AS src_tags,
           timeSeriesCopyTags(dest_tags, src_tags, ['__name__']) AS tags1,
           timeSeriesCopyTags(tags1, src_tags, ['code']) AS tags2,
           timeSeriesCopyTags(tags2, src_tags, ['code']) AS same_tags2,
           timeSeriesCopyTags(tags2, src_tags, ['unknown']) AS again_tags2,
           timeSeriesCopyTags(tags2, src_tags, ['env']) AS tags3,
           timeSeriesCopyTags(dest_tags, src_tags, ['__name__', 'code', 'unknown', 'env']) AS same_tags3,
           timeSeriesCopyTags(dest_tags, src_tags, ['code', 'code', 'env', '__name__', 'code']) AS again_tags3,
           timeSeriesCopyTags(dest_tags, src_tags, []) AS same_dest_tags
);

SELECT '';
SELECT 'timeSeriesRemoveTag:';

SELECT tags1,
       tags2,
       same_tags2 == tags2,
       empty_tags,
       empty(empty_tags)
FROM
(
    SELECT [('region', 'eu'), ('env', 'dev'), ('__name__', 'http_requests_count')] AS src_tags,
           timeSeriesRemoveTag(src_tags, '__name__') AS tags1,
           timeSeriesRemoveTag(tags1, 'env') AS tags2,
           timeSeriesRemoveTag(tags2, 'env') AS same_tags2,
           timeSeriesRemoveTag(tags2, 'region') AS empty_tags
);

SELECT '';
SELECT 'timeSeriesRemoveTags:';

SELECT tags1,
       tags2,
       same_tags2 == tags2,
       again_tags2 == tags2,
       empty_tags,
       empty(empty_tags),
       same_src_tags == src_tags
FROM
(
    SELECT [('__name__', 'http_requests_count'), ('env', 'dev'), ('region', 'eu')] AS src_tags,
           timeSeriesRemoveTags(src_tags, ['__name__']) AS tags1,
           timeSeriesRemoveTags(tags1, ['env']) AS tags2,
           timeSeriesRemoveTags(tags2, ['env']) AS same_tags2,
           timeSeriesRemoveTags(src_tags, ['__name__', 'env']) AS again_tags2,
           timeSeriesRemoveTags(tags2, ['region']) AS empty_tags,
           timeSeriesRemoveTags(src_tags, []) AS same_src_tags
);

SELECT '';
SELECT 'timeSeriesRemoveAllTagsExcept:';

SELECT tags1,
       tags2,
       same_tags2 == tags2,
       again_tags2 == tags2,
       empty_tags,
       empty(empty_tags),
       empty(again_empty_tags)
FROM
(
    SELECT [('region', 'eu'), ('env', 'dev'), ('__name__', 'http_requests_count')] AS src_tags,
           timeSeriesRemoveAllTagsExcept(src_tags, ['region', 'env']) AS tags1,
           timeSeriesRemoveAllTagsExcept(tags1, ['region']) AS tags2,
           timeSeriesRemoveAllTagsExcept(tags2, ['region']) AS same_tags2,
           timeSeriesRemoveAllTagsExcept(src_tags, ['region']) AS again_tags2,
           timeSeriesRemoveAllTagsExcept(tags2, ['unknown']) AS empty_tags,
           timeSeriesRemoveAllTagsExcept(tags2, []) AS again_empty_tags
);

SELECT '';
SELECT 'timeSeriesJoinTags:';

SELECT tags1,
       tags2,
       tags3,
       tags4,
       tags5,
       tags6,
       tags7,
       tags8
FROM
(
    SELECT [('__name__', 'up'), ('job', 'api-server'), ('src1', 'a'), ('src2', 'b'), ('src3', 'c')] AS src_tags,
           timeSeriesJoinTags(src_tags, 'foo', ',', ['src1', 'src2', 'src3']) AS tags1,
           timeSeriesJoinTags(src_tags, 'foo', '', ['src1', 'src2', 'src3']) AS tags2,
           timeSeriesJoinTags(src_tags, 'foo', ' sep ', ['src1', 'src2', 'src3']) AS tags3,
           timeSeriesJoinTags(src_tags, 'foo', ',', []) AS tags4,
           timeSeriesJoinTags(src_tags, 'foo', ',', ['src1']) AS tags5,
           timeSeriesJoinTags(src_tags, 'foo', ',', ['src1', 'src2', 'src1', 'src4']) AS tags6,
           timeSeriesJoinTags(src_tags, 'job', ',', ['src1', 'src2', 'src3']) AS tags7,
           timeSeriesJoinTags(src_tags, 'job', '', []) AS tags8
);

SELECT '';
SELECT 'timeSeriesReplaceTag:';

SELECT tags1,
       tags2,
       tags3,
       tags4,
       tags5,
       tags6,
       same_src_tags == src_tags,
       again_src_tags == src_tags,
       once_again_src_tags == src_tags
FROM
(
    SELECT [('__name__', 'up'), ('job', 'api-server'), ('service', 'a:c')] AS src_tags,
           timeSeriesReplaceTag(src_tags, 'foo', '$1', 'service', '(.*):.*') AS tags1,
           timeSeriesReplaceTag(src_tags, 'foo', '$0', 'service', '(.*):.*') AS tags2,
           timeSeriesReplaceTag(src_tags, 'foo', '$name', 'service', '(?P<name>.*):(?P<version>.*)') AS tags3,
           timeSeriesReplaceTag(src_tags, 'foo', 'begin ${name} sep ${version} end', 'service', '(?P<name>.*):(?P<version>.*)') AS tags4,
           timeSeriesReplaceTag(src_tags, 'service', '${2}:${1}', 'service', '(.*):(.*)') AS tags5,
           timeSeriesReplaceTag(src_tags, 'service', '', 'service', '(.*):(.*)') AS tags6,
           timeSeriesReplaceTag(src_tags, 'foo', '$0', 'service', 'unknown.*') AS same_src_tags,
           timeSeriesReplaceTag(src_tags, 'service', '$0', 'service', 'unknown.*') AS again_src_tags,
           timeSeriesReplaceTag(src_tags, 'foo', '$0', 'service', ':') AS once_again_src_tags
);

SELECT '';
SELECT 'timeSeriesReplaceTag (2):';

-- Multi-digit numeric refs ($11, ${11}) reference group #11.
-- A leading-zero name ($01, ${01}) is treated as a named-group reference, not a numeric one
-- An all-digit name with more than 9 digits exceeds the numeric cap and is treated as a named reference.
-- $$ becomes a literal '$'. A trailing '$' or a '$' followed by no name characters is also literal.
SELECT tags_eleventh,
       tags_eleventh_braced,
       tags_leading_zero_unbraced == seq_tags,
       tags_leading_zero_braced == seq_tags,
       tags_at_cap == seq_tags,
       tags_above_cap,
       tags_literal_dollar,
       tags_trailing_dollar,
       tags_dollar_then_punct
FROM
(
    SELECT [('seq', '0123456789a')] AS seq_tags,
           timeSeriesReplaceTag(seq_tags, 'eleventh', '$11', 'seq', '(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)') AS tags_eleventh,
           timeSeriesReplaceTag(seq_tags, 'eleventh_braced', '${11}', 'seq', '(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)(.)') AS tags_eleventh_braced,
           timeSeriesReplaceTag(seq_tags, 'leading_zero', '$01', 'seq', '(.*)') AS tags_leading_zero_unbraced,
           timeSeriesReplaceTag(seq_tags, 'leading_zero', '${01}', 'seq', '(.*)') AS tags_leading_zero_braced,
           timeSeriesReplaceTag(seq_tags, 'big', '$999999999', 'seq', '(?P<999999999>.+)') AS tags_at_cap,
           timeSeriesReplaceTag(seq_tags, 'big', '$1000000000', 'seq', '(?P<1000000000>.+)') AS tags_above_cap,
           timeSeriesReplaceTag(seq_tags, 'literal_dollar', '$$1', 'seq', '(.*)') AS tags_literal_dollar,
           timeSeriesReplaceTag(seq_tags, 'trailing_dollar', 'price$', 'seq', '(.*)') AS tags_trailing_dollar,
           timeSeriesReplaceTag(seq_tags, 'dollar_then_punct', '$.tail', 'seq', '(.*)') AS tags_dollar_then_punct
);

SELECT '';
SELECT 'timeSeriesThrowDuplicateSeriesIf:';

SELECT timeSeriesThrowDuplicateSeriesIf(0, tags)
FROM
(
    SELECT [('__name__', 'up')] AS tags
);

SELECT timeSeriesThrowDuplicateSeriesIf(1, tags)
FROM
(
    SELECT [('__name__', 'up')] AS tags
);  -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '';
SELECT 'timeSeriesThrowDuplicateSeriesIf vectorized all-zero:';

SELECT timeSeriesThrowDuplicateSeriesIf(materialize(toUInt8(0)), tags)
FROM
(
    SELECT [('__name__', 'up')] AS tags
    FROM numbers(4)
);

SELECT '';
SELECT 'timeSeriesThrowDuplicateSeriesIf negative zero:';

SELECT timeSeriesThrowDuplicateSeriesIf(materialize(CAST('-0.0', 'Float64')), tags)
FROM
(
    SELECT [('__name__', 'up')] AS tags
    FROM numbers(2)
);

SELECT '';
SELECT 'timeSeriesThrowDuplicateSeriesIf vectorized nonzero:';

SELECT timeSeriesThrowDuplicateSeriesIf(number = 2, tags)
FROM
(
    SELECT number,
           [('__name__', 'up'), ('instance', toString(number))] AS tags
    FROM numbers(4)
);  -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '';
SELECT 'timeSeriesRemoveTag vectorized:';

SELECT new_tags
FROM
(
    SELECT number,
           [('__name__', 'metric'), ('instance', toString(number))] AS old_tags,
           timeSeriesRemoveTag(old_tags, '__name__') AS new_tags
    FROM numbers(6)
)
ORDER BY number;

SELECT '';
SELECT 'timeSeriesRemoveTag vectorized from an array of tag sets:';

WITH
    (
        SELECT groupArray(tags)
        FROM
        (
            SELECT number,
                   [('__name__', 'metric'), ('instance', toString(number))] AS tags
            FROM numbers(32)
            ORDER BY number
        )
    ) AS tag_sets
SELECT timeSeriesRemoveTag(tag_sets[number * 8 + 1], '__name__')
FROM numbers(4)
ORDER BY number;

SELECT '';
SELECT 'timeSeriesRemoveAllTagsExcept vectorized repeated tag sets:';

WITH
    (
        SELECT groupArray(tags)
        FROM
        (
            SELECT number,
                   [('shared', 'value'), ('unique', toString(number))] AS tags
            FROM numbers(3)
            ORDER BY number
        )
    ) AS tag_sets
SELECT count(),
       uniqExact(new_tags),
       groupUniqArray(new_tags)
FROM
(
    SELECT timeSeriesRemoveAllTagsExcept(tag_sets[[1, 2, 1, 3, 2, 1][number + 1]], ['shared']) AS new_tags
    FROM numbers(6)
);

SELECT '';
SELECT 'timeSeriesRemoveAllTagsExcept vectorized repeated tag sets (2):';

WITH
    (
        SELECT groupArray(tags)
        FROM
        (
            SELECT number,
                   [('shared', 'value'), ('unique', toString(number))] AS tags
            FROM numbers(32)
            ORDER BY number
        )
    ) AS tag_sets
SELECT count(),
       uniqExact(new_tags),
       groupUniqArray(new_tags)
FROM
(
    SELECT timeSeriesRemoveAllTagsExcept(tag_sets[[1, 25, 1, 25, 1][number + 1]], ['shared']) AS new_tags
    FROM numbers(5)
);
