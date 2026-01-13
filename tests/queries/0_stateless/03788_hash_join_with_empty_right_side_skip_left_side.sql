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
