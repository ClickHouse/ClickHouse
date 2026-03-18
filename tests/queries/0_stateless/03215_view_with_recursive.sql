SET allow_experimental_analyzer = 1;

CREATE VIEW 03215_test_v
AS WITH RECURSIVE test_table AS
    (
        SELECT 1 AS number
        UNION ALL
        SELECT number + 1
        FROM test_table
        WHERE number < 100
    )
SELECT sum(number)
FROM test_table;

SELECT * FROM 03215_test_v;

CREATE VIEW 03215_multi_v
AS WITH RECURSIVE
    task AS
    (
        SELECT
            number AS task_id,
            number - 1 AS parent_id
        FROM numbers(10)
    ),
    rtq AS
    (
        SELECT
            task_id,
            parent_id
        FROM task AS t
        WHERE t.parent_id = 1
        UNION ALL
        SELECT
            t.task_id,
            t.parent_id
        FROM task AS t, rtq AS r
        WHERE t.parent_id = r.task_id
    )
SELECT count()
FROM rtq;

SELECT * FROM 03215_multi_v;
