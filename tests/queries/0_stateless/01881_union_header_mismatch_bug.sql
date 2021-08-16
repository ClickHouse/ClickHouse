select *　from (　select 'table' as table, toInt64(10) as rows, toInt64(101) as elements　union all　select 'another table' as table, toInt64(0) as rows, toInt64(0) as elements　)　where rows - elements <> 0;

SELECT
    label,
    number
FROM
(
    SELECT
        'a' AS label,
        number
    FROM
    (
        SELECT number
        FROM numbers(10)
    )
    UNION ALL
    SELECT
        'b' AS label,
        number
    FROM
    (
        SELECT number
        FROM numbers(10)
    )
)
WHERE number IN
(
    SELECT number
    FROM numbers(5)
) order by label, number;
