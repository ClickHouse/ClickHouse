SELECT DISTINCT t
FROM
(
    SELECT CAST('[(1, \'a\')]', 'String') AS t
    GROUP BY
        GROUPING SETS (
            (1),
            (printf(printf(NULL, 7, printf(isNullable(7), 7, '%%d: %d', 7, 7, 7, NULL), 7, materialize(toUInt256(7)), '%%d: %d', toNullable(7) IS NULL, 7), materialize(toNullable(NULL)))),
            (isZeroOrNull(isNullable(7)) IS NULL))
) AS na,
(
    SELECT CAST(toNullable('[(1, \'a\')]'), 'String') AS t
    GROUP BY
        1,
        toNullable(toUInt256(123)),
        printf(printf(NULL, *, printf(NULL, 7, 7, 7, '%%d: %d', 7, 7, materialize(7), 7, 7), 7, 7, '%%d: %d', 7, 7, 7))
)
SETTINGS joined_subquery_requires_alias = 0;
