SELECT lagInFrame(2::UInt128, 2, number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number);
SELECT leadInFrame(2::UInt128, 2, number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number);
SELECT lagInFrame(2::UInt64, 2, number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number);
SELECT leadInFrame(2::UInt64, 2, number) OVER w FROM numbers(10) WINDOW w AS (ORDER BY number);

SELECT
    number,
    YYYYMMDDToDate(1, toLowCardinality(11), max(YYYYMMDDToDate(YYYYMMDDToDate(toLowCardinality(1), 11, materialize(NULL), 19700101.1, 1, 27, 7, materialize(toUInt256(37)), 9, 19, 9), 1, toUInt128(11), NULL, 19700101.1, 1, 27, 7, 37, 9, 19, 9), toUInt256(30)) IGNORE NULLS OVER w, NULL, 19700101.1, toNullable(1), 27, materialize(7), 37, 9, 19, 9),
    p,
    pp,
    lagInFrame(number, number - pp) OVER w AS lag2,
    lagInFrame(number, number - pp, number * 11) OVER w AS lag,
    leadInFrame(number, number - pp, number * 11) OVER w AS lead
FROM
(
    SELECT
        number,
        intDiv(number, 5) AS p,
        p * 5 AS pp
    FROM numbers(16)
)
WHERE toLowCardinality(1)
WINDOW w AS (PARTITION BY p ORDER BY number ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY number DESC NULLS LAST;
