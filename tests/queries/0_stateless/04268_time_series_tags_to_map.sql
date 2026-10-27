-- Tests for the timeSeriesTagsToMap scalar function.

SELECT 'Combine the tags array with separate tags, sorted, including __name__:';
SELECT timeSeriesTagsToMap([('region', 'eu'), ('env', 'dev')], '__name__', 'http_requests_count');

SELECT 'A Map is accepted as the tags array:';
SELECT timeSeriesTagsToMap(map('region', 'eu', 'env', 'dev'), '__name__', 'http_requests_count');

SELECT 'Only the tags array, no separate pairs:';
SELECT timeSeriesTagsToMap([('b', '2'), ('a', '1')]);

SELECT 'A duplicate tag name is removed:';
SELECT timeSeriesTagsToMap([('job', 'api')], 'job', 'api');

SELECT 'A tag with an empty value is dropped:';
SELECT timeSeriesTagsToMap([('region', 'eu')], 'instance', '');

SELECT 'A NULL tag value means the tag is absent:';
SELECT timeSeriesTagsToMap([('region', 'eu')], 'instance', NULL);

SELECT 'An empty separate value does not conflict with the same tag in the array (the array value wins):';
SELECT timeSeriesTagsToMap(map('__name__', 'bar', 'x', '1'), '__name__', '');

SELECT 'Empty input produces an empty map:';
SELECT timeSeriesTagsToMap(CAST([], 'Array(Tuple(String, String))'));

SELECT 'FixedString values in the tags array are normalized:';

WITH
    timeSeriesTagsToMap(
        CAST([('a', 'x')], 'Array(Tuple(FixedString(3), FixedString(3)))')
    ) AS from_array,
    timeSeriesTagsToMap([], toFixedString('a', 3), toFixedString('x', 3)) AS from_arguments
SELECT from_array = from_arguments,
       mapContains(from_array, 'a'),
       mapContains(from_arguments, 'a');
