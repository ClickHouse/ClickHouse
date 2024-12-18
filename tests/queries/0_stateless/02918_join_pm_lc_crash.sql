
SET joined_subquery_requires_alias = 0, join_algorithm = 'partial_merge';

SET allow_experimental_analyzer = 0, join_use_nulls = 0;

SELECT * FROM (SELECT dummy AS val FROM system.one)
JOIN (SELECT toLowCardinality(toNullable(dummy)) AS val
FROM system.one GROUP BY val WITH TOTALS)
USING (val);

SET allow_experimental_analyzer = 0, join_use_nulls = 1;

SELECT * FROM (SELECT dummy AS val FROM system.one)
JOIN (SELECT toLowCardinality(toNullable(dummy)) AS val
FROM system.one GROUP BY val WITH TOTALS)
USING (val);

SET allow_experimental_analyzer = 1, join_use_nulls = 0;

SELECT * FROM (SELECT dummy AS val FROM system.one)
JOIN (SELECT toLowCardinality(toNullable(dummy)) AS val
FROM system.one GROUP BY val WITH TOTALS)
USING (val);

SET allow_experimental_analyzer = 1, join_use_nulls = 1;

SELECT * FROM (SELECT dummy AS val FROM system.one)
JOIN (SELECT toLowCardinality(toNullable(dummy)) AS val
FROM system.one GROUP BY val WITH TOTALS)
USING (val);

