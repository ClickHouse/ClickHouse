SELECT
    sumMapIf([1], [1], nullIf(number, 3) > 0) AS col1,
    countIf(1, nullIf(number, 3) > 0) AS col2,
    sumIf(1, nullIf(number, 3) > 0) AS col3
FROM numbers(1, 5);
