SELECT
    sum(c),
    toInt32((h - null::Nullable(DateTime)) / 3600) + 1 AS a
FROM
(
    SELECT count() AS c, h
    FROM ( SELECT now() AS h )
    WHERE toInt32((h - null::Nullable(DateTime)) / 3600) + 1 = 1
    GROUP BY h
)
GROUP BY a settings min_count_to_compile_expression = 0;
