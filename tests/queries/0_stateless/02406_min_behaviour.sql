WITH
    arrayJoin([1, 2, 3, nan, 4, 5]) AS data,
    arrayJoin([nan, 1, 2, 3, 4]) AS data2,
    arrayJoin([1, 2, 3, 4, nan]) AS data3,
    arrayJoin([nan, nan, nan]) AS data4,
    arrayJoin([nan, 1, 2, 3, nan]) AS data5
SELECT
    min(data),
    min(data2),
    min(data3),
    min(data4),
    min(data5);

SELECT minIf(number, rand() % 2 == 3) from numbers(10);
SELECT minIf(number::Float64, rand() % 2 == 3) from numbers(10);

-- { echoOn }
SET compile_aggregate_expressions=1;
SET min_count_to_compile_aggregate_expression=0;
-- { echoOff }

WITH
    arrayJoin([1, 2, 3, nan, 4, 5]) AS data,
    arrayJoin([nan, 1, 2, 3, 4]) AS data2,
    arrayJoin([1, 2, 3, 4, nan]) AS data3,
    arrayJoin([nan, nan, nan]) AS data4,
    arrayJoin([nan, 1, 2, 3, nan]) AS data5
SELECT
    min(data),
    min(data2),
    min(data3),
    min(data4),
    min(data5);

SELECT minIf(number, rand() % 2 == 3) from numbers(10);
SELECT minIf(number::Float64, rand() % 2 == 3) from numbers(10);
