SELECT
    dummy,
    SumDum,
    ProblemField
FROM
(
    SELECT
        dummy,
        sum(dummy) AS SumDum,
        1 / SumDum AS ProblemField
    FROM system.one
    GROUP BY dummy
    ORDER BY
        dummy ASC,
        SumDum ASC,
        CAST(ifNull(ProblemField, -inf) AS Float64) ASC
);
