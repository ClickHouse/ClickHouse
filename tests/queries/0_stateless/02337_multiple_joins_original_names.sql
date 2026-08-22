-- https://github.com/ClickHouse/ClickHouse/issues/34697

SET enable_analyzer = 1;

SELECT table1_id FROM (
    SELECT first.table1_id
    FROM (SELECT number+1 as table1_id FROM numbers(1)) as first
    JOIN (SELECT number+1 as table2_id FROM numbers(1)) as second ON first.table1_id = second.table2_id
    JOIN (SELECT number+1 as table3_id FROM numbers(1)) as third ON first.table1_id = third.table3_id
);

SELECT table1_id FROM (
    SELECT first.table1_id
    FROM (SELECT number+1 as table1_id FROM numbers(1)) as first
    JOIN (SELECT number+1 as table2_id FROM numbers(1)) as second ON first.table1_id = second.table2_id
    JOIN (SELECT number+1 as table3_id FROM numbers(1)) as third ON first.table1_id = third.table3_id
) SETTINGS multiple_joins_try_to_keep_original_names = 1;

SELECT aaa FROM (
    SELECT first.table1_id as aaa
    FROM (SELECT number+1 as table1_id FROM numbers(1)) as first
    JOIN (SELECT number+1 as table2_id FROM numbers(1)) as second ON first.table1_id = second.table2_id
    JOIN (SELECT number+1 as table3_id FROM numbers(1)) as third ON first.table1_id = third.table3_id
) SETTINGS multiple_joins_try_to_keep_original_names = 1;
