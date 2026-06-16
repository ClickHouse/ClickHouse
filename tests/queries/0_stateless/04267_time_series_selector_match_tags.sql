-- Tests for the timeSeriesSelectorMatchTags scalar function.

SELECT 'Equality/inequality matchers:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", job="api"}', [('job', 'api')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job="api"}', [('job', 'web')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job!="api"}', [('job', 'web')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job!="api"}', [('job', 'api')], '__name__', 'up');

SELECT 'Regex matcher:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", job=~"api.*"}', [('job', 'api-v1')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job=~"api.*"}', [('job', 'web')],    '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job!~"api.*"}', [('job', 'web')],    '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", job!~"api.*"}', [('job', 'api-v1')], '__name__', 'up');

SELECT 'Empty tag value:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", missing=""}',  [('job', 'api')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", missing!=""}', [('job', 'api')], '__name__', 'up');
SELECT timeSeriesSelectorMatchTags('{__name__="up", missing="x"}', [('job', 'api')], '__name__', 'up');

SELECT 'All tags in array:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", instance="a"}', [('__name__', 'up'), ('instance', 'a')]);

SELECT 'All tags as pairs:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", instance="a"}', [], '__name__', 'up', 'instance', 'a');

SELECT 'Non-const tag value:';
SELECT job, timeSeriesSelectorMatchTags('{__name__="up", job=~"api.*"}', [('job', job)], '__name__', 'up') AS matched
FROM
(
    SELECT arrayJoin(['api', 'api-v1', 'api-v2', 'web', 'db']) AS job
)
ORDER BY job;

SELECT 'Map(String, String) instead of array:';
SELECT timeSeriesSelectorMatchTags('{__name__="up", job="api"}', map('job', 'api'), '__name__', 'up');

SELECT 'Non-instant selector is rejected:';
SELECT timeSeriesSelectorMatchTags('sum(up)', [], '__name__', 'up'); -- { serverError BAD_ARGUMENTS }
