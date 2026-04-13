SET group_by_use_nulls = 1;

SELECT intDiv(bitNot((SELECT bitNot(number))), 65536)
FROM numbers(1)
GROUP BY GROUPING SETS (
    (bitNot(bitNot(number))), (), (),
    (*, '\0', bitNot(bitCount(number)) - (SELECT NULL LIMIT 1048577),
     toLowCardinality(toNullable('10.000100')))
)
ORDER BY ALL;
