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