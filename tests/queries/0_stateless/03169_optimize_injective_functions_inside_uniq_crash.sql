SELECT sum(u)
FROM
(
    SELECT
        intDiv(number, 4096) AS k,
        uniqCombined(tuple(materialize(toLowCardinality(toNullable(16))))) AS u
    FROM numbers(4096 * 100)
    GROUP BY k
)
SETTINGS enable_analyzer = 1, optimize_injective_functions_inside_uniq=0;

SELECT sum(u)
FROM
(
    SELECT
        intDiv(number, 4096) AS k,
        uniqCombined(tuple(materialize(toLowCardinality(toNullable(16))))) AS u
    FROM numbers(4096 * 100)
    GROUP BY k
)
SETTINGS enable_analyzer = 1, optimize_injective_functions_inside_uniq=1;
