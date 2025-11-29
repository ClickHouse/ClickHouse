CREATE TABLE mytable(a Array(Nullable(Float64))) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO mytable VALUES ([100, NULL, NULL]);
INSERT INTO mytable VALUES ([NULL, 200, NULL]);
INSERT INTO mytable VALUES ([NULL, NULL, 300]);
SELECT timeSeriesCoalesceGridValues('throw_if_conflict')(a) AS result FROM mytable;
DROP TABLE mytable;

WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data SELECT timeSeriesCoalesceGridValues('null_if_conflict')(arrayJoin(data));
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data SELECT timeSeriesCoalesceGridValues('throw_if_conflict')(arrayJoin(data)); -- {serverError BAD_ARGUMENTS}
