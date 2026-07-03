SET enable_analyzer=1;

SELECT * FROM (SELECT 1 AS a, 2 AS b FROM system.one INNER JOIN system.one USING (dummy) UNION ALL SELECT 3 AS a, 4 AS b FROM system.one) WHERE a != 10 ORDER BY a ASC, a != 10 ASC, b ASC;

SELECT 4
FROM
(
    SELECT
        1 AS X,
        2 AS Y
    UNION ALL
    SELECT
        3,
        4
    GROUP BY 2
)
WHERE materialize(4)
ORDER BY materialize(4) ASC NULLS LAST;

SELECT *
FROM
(
    SELECT 1 AS a
    GROUP BY
        GROUPING SETS ((tuple(toUInt128(67))))
    UNION ALL
    SELECT materialize(2)
)
WHERE a
ORDER BY (75, ((tuple(((67, (67, (tuple((tuple(toLowCardinality(toLowCardinality(1))), 1)), toNullable(1))), (tuple(toUInt256(1)), 1)), 1)), 1), 1), toNullable(1)) ASC
FORMAT Pretty;
