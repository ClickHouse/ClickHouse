WITH
    table_x AS (
        SELECT col_a, col_b
        FROM (SELECT 'a' AS col_a, ['b', 'c'] AS col_b)
        ARRAY JOIN col_b
    ),
    (SELECT groupArray((col_a, col_b)) FROM table_x) AS group_a
SELECT group_a, groupArray((col_a, col_b)) FROM table_x;
