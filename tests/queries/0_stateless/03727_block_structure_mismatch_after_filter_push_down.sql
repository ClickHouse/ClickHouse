SELECT *
FROM
(
    SELECT materialize(toUInt256(2))
    UNION ALL
    SELECT DISTINCT 1
)
GROUP BY
    ignore(lessOrEquals(18, isNotNull(toLowCardinality(10)))),
    1
HAVING ignore(isZeroOrNull(5));
