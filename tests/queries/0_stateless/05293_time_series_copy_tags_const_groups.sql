-- timeSeriesCopyTag and timeSeriesCopyTags return one row per input row when both tags arguments are constant.

SELECT timeSeriesCopyTags([], [], ['src']);
SELECT timeSeriesCopyTag([], [], 'src');

SELECT timeSeriesCopyTags([], [], ['src']) FROM numbers(3);
SELECT timeSeriesCopyTag([], [], 'src') FROM numbers(3);

SELECT timeSeriesCopyTags(identity([]), [], ['src']) FROM numbers(3);
SELECT timeSeriesCopyTags(NULL, [], ['src']) FROM numbers(3);
SELECT timeSeriesCopyTag([], CAST(map(), 'Map(String, String)'), 'src') FROM numbers(3);

WITH (SELECT [('region', 'eu'), ('__name__', 'dest_metric')]) AS dest_tags,
     (SELECT [('code', '404'), ('__name__', 'src_metric')]) AS src_tags
SELECT timeSeriesCopyTags(dest_tags, src_tags, ['__name__', 'code']),
       timeSeriesCopyTag(dest_tags, src_tags, 'code')
FROM numbers(3);
