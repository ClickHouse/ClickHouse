SELECT
    number,
    sum(number) OVER (ORDER BY number ASC RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) AS sum
FROM values('number Nullable(Int8)', 1, 1, 2, 3, NULL)