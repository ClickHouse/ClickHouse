SET optimize_if_transform_const_strings_to_lowcardinality = 1;

SELECT 'With optimize_if_transform_strings_to_enum = 0';
SET optimize_if_transform_strings_to_enum = 0;

SELECT 'if';
SELECT if(number % 2 = 0, 'x', 'y') as col, toTypeName(col) FROM numbers(3);
SELECT if(materialize(number % 2 = 0), 'x', 'y') as col, toTypeName(col) FROM numbers(3);

SELECT 'if strings expected to work';
SELECT number AS n, if(n % 2 == 0, 'Even', 'Odd') as txt, toTypeName(txt) FROM numbers(4);
SELECT number AS n, if(materialize(n % 2 == 0), 'Even', 'Odd') as txt, toTypeName(txt) FROM numbers(4);
SELECT number AS n, if(n % 2 == 0, NULL, 'Odd') as txt, toTypeName(txt) FROM numbers(4);
SELECT number AS n, if(materialize(n % 2 == 0), NULL, 'Odd') as txt, toTypeName(txt) FROM numbers(4);
SELECT 'if strings not expected to work';
SELECT number AS n, if(materialize(n % 2 == 0), materialize('Even'), 'Odd') as txt, toTypeName(txt) FROM numbers(4);
SELECT number AS n, if(materialize(n % 2 == 0), materialize('Even'), materialize('Odd')) as txt, toTypeName(txt) FROM numbers(4);
SELECT number AS n, if(materialize(n % 2 == 0), NULL, materialize('Odd')) as txt, toTypeName(txt) FROM numbers(4);


SELECT 'multiIf';
SELECT multiIf(number % 2 = 0, 'a', number = 1, 'NUM', 'hello') as col, toTypeName(col) FROM numbers(4);
SELECT multiIf(materialize(number % 2 = 0), 'a', number = 1, 'NUM', 'hello') as col, toTypeName(col) FROM numbers(4);

SELECT 'multiIf strings expected to work';
SELECT
    number + 1 AS n,
    multiIf(
        (number + 1) % 15 = 0, 'FizzBuzz',
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, 'Buzz',
        'Normal')
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT
    materialize(number + 1) AS n,
    multiIf(
        (number + 1) % 15 = 0, 'FizzBuzz',
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, 'Buzz',
        'Normal')
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT
    number + 1 AS n,
    multiIf(
        (number + 1) % 15 = 0, 'FizzBuzz',
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, 'Buzz',
        NULL)
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT
    materialize(number + 1) AS n,
    multiIf(
        (number + 1) % 15 = 0, 'FizzBuzz',
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, 'Buzz',
        NULL)
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT 'multiIf strings not expected to work';

SELECT
    materialize(number + 1) AS n,
    multiIf(
        (number + 1) % 15 = 0, materialize('FizzBuzz'),
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, 'Buzz',
        'Normal')
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT
    materialize(number + 1) AS n,
    multiIf(
        (number + 1) % 15 = 0, 'FizzBuzz',
        (number + 1) % 3 = 0, 'Fizz',
        (number + 1) % 5 = 0, materialize('Buzz'),
        NULL)
    AS txt,
    toTypeName(txt)
FROM numbers(15);

SELECT 'transform with default - should become LowCardinality';
SELECT 
    user_id,
    country_code,
    transform(country_code, ['US', 'GB', 'DE'], ['United States', 'United Kingdom', 'Germany'], 'Unknown') AS country_name, toTypeName(country_name)
FROM (
    SELECT 
        number + 100 AS user_id,
        ['US', 'GB', 'DE', 'FR', 'IT'][number % 5 + 1] AS country_code
    FROM numbers(10)
);

SELECT 'transform with default and NULLs - should become LowCardinality(Nullable(String))';
SELECT 
    user_id,
    country_code,
    transform(country_code, ['US', 'GB', 'DE', NULL], ['United States', 'United Kingdom', 'Germany', NULL], 'Unknown') AS country_name, toTypeName(country_name)
FROM (
    SELECT 
        number + 100 AS user_id,
        ['US', 'GB', 'DE', 'FR', 'IT', NULL][number % 5 + 1] AS country_code
    FROM numbers(10)
);

SELECT 'transform with default * - leave String';
SELECT transform(country_code, ['US', 'DE'], ['United States', 'Germany'], *) as res, toTypeName(res)
FROM
(
    SELECT ['US', 'GB', 'DE', 'FR', 'IT'][number % 5 + 1] AS country_code FROM numbers(10)
) SETTINGS enable_analyzer = 1;

SELECT 'transform without default - leave String';
SELECT 
    user_id,
    country_code,
    transform(country_code, ['US', 'GB', 'DE'], ['United States', 'United Kingdom', 'Germany']) AS country_name, toTypeName(country_name)
FROM (
    SELECT 
        number + 100 AS user_id,
        ['US', 'GB', 'DE', 'FR', 'IT'][number % 5 + 1] AS country_code
    FROM numbers(10)
);

SELECT 'From 02366_kql_func_dynamic.sql';
SELECT if((NULL IS NULL) AND 
   ((extract(toTypeName(['a', 'b', 'c']), 'Array\\((.*)\\)') AS element_type_3315726463) = 'String'),
   defaultValueOfTypeName(
       if(element_type_3315726463 = 'Nothing', 
          'Nullable(Nothing)', 
          element_type_3315726463)
   ),
   NULL) AS fill_value_3315726463;


SELECT 'toBool(LowCardinality(Nullable(String))) from if';
SELECT toBool(if(number % 2, 'true', NULL)) as x, toTypeName(x) from numbers(2);

SELECT 'multiIf weirdness';
SELECT DISTINCT multiIf(materialize(toLowCardinality(0)), 'A', 0 = number, 'B', 'C') AS txt, toTypeName(txt) FROM numbers(15);


SELECT 'With optimize_if_transform_strings_to_enum = 1';
SET optimize_if_transform_strings_to_enum = 1;

SELECT 'if';
SELECT if(number % 2 = 0, 'x', 'y') as col, toTypeName(col) FROM numbers(3);
SELECT if(materialize(number % 2 = 0), 'x', 'y') as col, toTypeName(col) FROM numbers(3);

SELECT 'multiIf';
SELECT multiIf(number % 2 = 0, 'a', number = 1, 'NUM', 'hello') as col, toTypeName(col) FROM numbers(4);
SELECT multiIf(materialize(number % 2 = 0), 'a', number = 1, 'NUM', 'hello') as col, toTypeName(col) FROM numbers(4);

SELECT 'transform with default';
SELECT 
    user_id,
    country_code,
    transform(country_code, ['US', 'GB', 'DE'], ['United States', 'United Kingdom', 'Germany'], 'Unknown') AS country_name, toTypeName(country_name)
FROM (
    SELECT 
        number + 100 AS user_id,
        ['US', 'GB', 'DE', 'FR', 'IT'][number % 5 + 1] AS country_code
    FROM numbers(10)
);

SELECT 'transform with default and NULLs';
SELECT 
    user_id,
    country_code,
    transform(country_code, ['US', 'GB', 'DE', NULL], ['United States', 'United Kingdom', 'Germany', NULL], 'Unknown') AS country_name, toTypeName(country_name)
FROM (
    SELECT 
        number + 100 AS user_id,
        ['US', 'GB', 'DE', 'FR', 'IT', NULL][number % 5 + 1] AS country_code
    FROM numbers(10)
);
