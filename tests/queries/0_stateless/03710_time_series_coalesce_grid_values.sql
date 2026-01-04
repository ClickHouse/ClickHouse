CREATE TABLE mytable(a Array(Nullable(Float64))) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO mytable VALUES ([100, NULL, NULL]);
INSERT INTO mytable VALUES ([NULL, 200, NULL]);
INSERT INTO mytable VALUES ([NULL, NULL, 300]);
SELECT timeSeriesCoalesceGridValues('throw')(a) AS result FROM mytable;
DROP TABLE mytable;


WITH data AS
    (
        SELECT arrayJoin([[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]]) AS values
    )
SELECT timeSeriesCoalesceGridValues('any')(values)
FROM data;


WITH data AS
    (
        SELECT arrayJoin([[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]]) AS values
    )
SELECT timeSeriesCoalesceGridValues('nan')(values)
FROM data;


WITH data AS
    (
        SELECT arrayJoin([[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]]) AS values
    )
SELECT timeSeriesCoalesceGridValues('throw')(values)
FROM data; -- {serverError CANNOT_EXECUTE_PROMQL_QUERY}


WITH
    (
        SELECT timeSeriesTagsToGroup([('__name__', 'up')])
    ) AS group1,
    (
        SELECT timeSeriesTagsToGroup([('__name__', 'http_errors')])
    ) AS group2,
    data AS
    (
        SELECT
            (arrayJoin([([1., NULL, 3., NULL, 5], group1), ([NULL, 2., NULL, NULL, 5], group2)]) AS t).1 AS values,
            t.2 AS group
    )
SELECT timeSeriesCoalesceGridValues('throw')(values, group)
FROM data; -- {serverError CANNOT_EXECUTE_PROMQL_QUERY}
