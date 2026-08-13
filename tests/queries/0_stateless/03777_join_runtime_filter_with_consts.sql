SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET enable_join_runtime_filters=1, join_runtime_filter_min_probe_rows = 0;

SELECT * FROM (SELECT 1 as a) t1
JOIN (SELECT 1 as a) as t2
ON t1.a = t2.a;
