SET join_use_nulls = true;
SET query_plan_use_new_logical_join_step = false;
SET limit = 8192;

SELECT ALL 4768537185765142272::Int16 AS a0
FROM generate_series(16650, 92847) AS t0d0
RIGHT JOIN generateSeries(57671, 92987) AS t1d0
USING (generate_series)
ORDER BY t0d0.generate_series
FORMAT NULL;
