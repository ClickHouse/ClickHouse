SELECT count()
FROM
    (SELECT number AS x FROM system.numbers WHERE x % 10 > 20) AS a -- all rows are read and filtered out
    INNER JOIN (SELECT toUInt64(1) AS y WHERE 0) AS b
    ON a.x = b.y
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0;

SELECT count()
FROM
    (SELECT number AS x FROM system.numbers WHERE x % 10 > 20) AS a -- all rows are read and filtered out
    INNER JOIN (SELECT toUInt64(1) AS y WHERE 0) AS b
    ON a.x = b.y
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0;

SELECT count()
FROM
    (SELECT number AS x FROM system.numbers) AS a -- all rows are read and filtered out by runtime filter
    INNER JOIN (SELECT toUInt64(1) AS y WHERE 0) AS b
    ON a.x = b.y
SETTINGS enable_join_runtime_filters = 1, query_plan_join_swap_table = 0;

SELECT count()
FROM
    (SELECT sum(number) AS x FROM system.numbers) AS a -- very heavy aggregation
    INNER JOIN (SELECT toUInt64(1) AS y WHERE 0) AS b
    ON a.x = b.y
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0;

-- CROSS JOIN
SELECT count()
FROM
    (SELECT sum(number) AS x FROM system.numbers) AS a -- very heavy aggregation
    CROSS JOIN (SELECT toUInt64(1) AS y WHERE 0) AS b
SETTINGS query_plan_join_swap_table = 0;

-- COMMA CROSS JOIN
SELECT count()
FROM
    (SELECT sum(number) AS x FROM system.numbers) AS a, -- very heavy aggregation
    (SELECT toUInt64(1) AS y WHERE 0) AS b
SETTINGS query_plan_join_swap_table = 0;

SELECT count() FROM numbers(1e12) t1, null('x UInt8') t2
SETTINGS query_plan_join_swap_table = 0, max_rows_to_read = 1e13;

SELECT count() FROM numbers(1e12) t1, numbers(1e12) t2, numbers(1e12) t3, numbers(1e12) t4, null('x UInt8') t5
SETTINGS query_plan_join_swap_table = 0, max_rows_to_read = 1e13;
