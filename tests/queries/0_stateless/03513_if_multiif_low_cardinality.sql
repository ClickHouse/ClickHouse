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

SELECT 'transform with default and NULLs- should become LowCardinality(Nullable(String))';
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

SELECT 'transform without default';
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
