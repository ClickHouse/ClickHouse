SELECT
    sum(ASD) AS asd,
    intDiv(toInt64(asd), abs(toInt64(asd))) AS int_div_with_abs,
    intDiv(toInt64(asd), toInt64(asd)) AS int_div_without_abs
FROM
(
    SELECT ASD
    FROM
    (
        SELECT [-1000, -1000] AS asds
    )
    ARRAY JOIN asds AS ASD
);

SELECT  intDivOrZero( CAST(-1000, 'Int64')   , CAST(1000, 'UInt64') );
SELECT  intDivOrZero( CAST(-1000, 'Int64')   , CAST(1000, 'Int64') );

SELECT intDiv(-1, number) FROM numbers(1, 10);
SELECT intDivOrZero(-1, number) FROM numbers(1, 10);
SELECT intDiv(toInt32(number), -1) FROM numbers(1, 10);
SELECT intDivOrZero(toInt32(number), -1) FROM numbers(1, 10);
SELECT intDiv(toInt64(number), -1) FROM numbers(1, 10);
SELECT intDivOrZero(toInt64(number), -1) FROM numbers(1, 10);
SELECT intDiv(number, -number) FROM numbers(1, 10);
SELECT intDivOrZero(number, -number) FROM numbers(1, 10);
