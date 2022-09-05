WITH
    arrayJoin([1, 2, 3, nan, 4, 5]) AS data,
    arrayJoin([nan, 1, 2, 3, 4]) AS data2,
    arrayJoin([1, 2, 3, 4, nan]) AS data3,
    arrayJoin([nan, nan, nan]) AS data4,
    arrayJoin([nan, 1, 2, 3, nan]) AS data5
SELECT
    'minNan',
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
    'maxNan',
    max(data),
    max(data2),
    max(data3),
    max(data4),
    max(data5);

SELECT 'minIf', minIf(number, rand() % 2 == 3) from numbers(10);
SELECT 'maxIf', maxIf(number, rand() % 2 == 3) from numbers(10);

SELECT 'minIf_FP', minIf(number::Float64, rand() % 2 == 3) from numbers(10);
SELECT 'maxIf_FP', maxIf(number::Float64, rand() % 2 == 3) from numbers(10);

SELECT 'minIf_String', minIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT 'maxIf_String', maxIf(number::String, number < 10) as number from numbers(10, 1000);

SELECT 'minIf_NullableString', minIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);
SELECT 'maxIf_NullableString', maxIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);

SELECT 'min_NullableString', min(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));
SELECT 'max_NullableString', max(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));


SELECT 'argmax_numeric_block1', argMax(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block2', argMax(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmax_numeric_block3', argMax(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block4', argMax(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmax_numeric_block5', argMax(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block6', argMax(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmax_numeric_block7', argMax(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block8', argMax(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmax_numeric_block9', argMaxIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block10', argMaxIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmax_numeric_block11', argMaxIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmax_numeric_block12', argMaxIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;

SELECT 'argmin_numeric_block1', argMin(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block2', argMin(number, now()) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmin_numeric_block3', argMin(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block4', argMin(number, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmin_numeric_block3', argMin(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block4', argMin(number::String, 1) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmin_numeric_block7', argMin(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block8', argMin(number, now() + number) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmin_numeric_block9', argMinIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block10', argMinIf(number, now() + number, number % 10 < 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;
SELECT 'argmin_numeric_block11', argMinIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=100;
SELECT 'argmin_numeric_block12', argMinIf(number, now() + number, number % 10 > 20) FROM (Select number as number from numbers(10, 10000)) settings max_threads=1, max_block_size=20000;

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
    'minNan',
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
    'maxNan',
    max(data),
    max(data2),
    max(data3),
    max(data4),
    max(data5);

SELECT 'minIf', minIf(number, rand() % 2 == 3) from numbers(10);
SELECT 'maxIf', maxIf(number, rand() % 2 == 3) from numbers(10);

SELECT 'minIf_FP', minIf(number::Float64, rand() % 2 == 3) from numbers(10);
SELECT 'maxIf_FP', maxIf(number::Float64, rand() % 2 == 3) from numbers(10);

SELECT 'minIf_String', minIf(number::String, number < 10) as number from numbers(10, 1000);
SELECT 'maxIf_String', maxIf(number::String, number < 10) as number from numbers(10, 1000);

SELECT 'minIf_NullableString', minIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);
SELECT 'maxIf_NullableString', maxIf(number::Nullable(String), number < 10) as number from numbers(10, 1000);

SELECT 'min_NullableString', min(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));
SELECT 'max_NullableString', max(n::Nullable(String)) from (Select if(number < 15 and number % 2 == 1, number * 2, NULL) as n from numbers(10, 20));
