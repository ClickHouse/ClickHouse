-- { echoOn }
SET compile_aggregate_expressions=0;

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

WITH
    arrayJoin([1, 2, 3, nan, 4, 5]) AS data,
    arrayJoin([nan, 1, 2, 3, 4]) AS data2,
    arrayJoin([1, 2, 3, 4, nan]) AS data3,
    arrayJoin([nan, nan, nan]) AS data4,
    arrayJoin([nan, 1, 2, 3, nan]) AS data5
SELECT
    max(data),
    max(data2),
    max(data3),
    max(data4),
    max(data5);

Select max(number) from numbers(100) settings max_threads=1, max_block_size=10;
Select max(-number) from numbers(100);
Select min(number) from numbers(100) settings max_threads=1, max_block_size=10;
Select min(-number) from numbers(100);

SELECT minIf(number, rand() % 2 == 3) from numbers(10) settings max_threads=1, max_block_size=5;
SELECT maxIf(number, rand() % 2 == 3) from numbers(10) settings max_threads=1, max_block_size=5;

SELECT minIf(number::Float64, rand() % 2 == 3) from numbers(10) settings max_threads=1, max_block_size=5;
SELECT maxIf(number::Float64, rand() % 2 == 3) from numbers(10) settings max_threads=1, max_block_size=5;

SELECT minIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::String, number % 3), maxIf(number::String, number % 5), minIf(number::String, number % 3), minIf(number::String, number > 10) from numbers(400);

SELECT minIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);

SELECT min(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));
SELECT max(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));

SELECT max(number) from (Select if(number % 2 == 1, NULL, -number::Int8) as number FROM numbers(128));
SELECT min(number) from (Select if(number % 2 == 1, NULL, -number::Int8) as number FROM numbers(128));

SELECT argMax(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMax(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMax(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMax(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMax(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMax(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMax(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMax(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMaxIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMaxIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMaxIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMaxIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMax(number, number::Float64) from numbers(2029);
SELECT argMaxIf(number, number::Float64, number > 2030) from numbers(2029);
SELECT argMaxIf(number, number::Float64, number > 2030) from numbers(2032);
SELECT argMax(number, -number::Float64) from numbers(2029);
SELECT argMaxIf(number, -number::Float64, number > 2030) from numbers(2029);
SELECT argMaxIf(number, -number::Float64, number > 2030) from numbers(2032);

SELECT argMin(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMin(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMin(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMin(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMin(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMin(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMin(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMin(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMinIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMinIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMinIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT argMinIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT argMin(number, number::Float64) from numbers(2029);
SELECT argMinIf(number, number::Float64, number > 2030) from numbers(2029);
SELECT argMinIf(number, number::Float64, number > 2030) from numbers(2032);
SELECT argMin(number, -number::Float64) from numbers(2029);
SELECT argMinIf(number, -number::Float64, number > 2030) from numbers(2029);
SELECT argMinIf(number, -number::Float64, number > 2030) from numbers(2032);

Select argMax((n, n), n) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));
Select argMaxIf((n, n), n, n < 5) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));
Select argMaxIf((n, n), n, n > 5) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));

Select argMin((n, n), n) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));
Select argMinIf((n, n), n, n < 5) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));
Select argMinIf((n, n), n, n > 5) t, toTypeName(t) FROM (Select if(number % 3 == 0, NULL, number) as n from numbers(10));

SET compile_aggregate_expressions=1;
SET min_count_to_compile_aggregate_expression=0;

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

WITH
    arrayJoin([1, 2, 3, nan, 4, 5]) AS data,
    arrayJoin([nan, 1, 2, 3, 4]) AS data2,
    arrayJoin([1, 2, 3, 4, nan]) AS data3,
    arrayJoin([nan, nan, nan]) AS data4,
    arrayJoin([nan, 1, 2, 3, nan]) AS data5
SELECT
    max(data),
    max(data2),
    max(data3),
    max(data4),
    max(data5);

SELECT minIf(number, rand() % 2 == 3) from numbers(10);
SELECT maxIf(number, rand() % 2 == 3) from numbers(10);

SELECT minIf(number::Float64, rand() % 2 == 3) from numbers(10);
SELECT maxIf(number::Float64, rand() % 2 == 3) from numbers(10);

SELECT minIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::String, number % 3), maxIf(number::String, number % 5), minIf(number::String, number % 3), minIf(number::String, number > 10) from numbers(400);

SELECT minIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);
SELECT maxIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);

SELECT min(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));
SELECT max(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));

SELECT max(number::Nullable(Decimal64(3))) from numbers(11) settings max_block_size=10;
SELECT min(-number::Nullable(Decimal64(3))) from numbers(11) settings max_block_size=10;
