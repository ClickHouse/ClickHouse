SET enable_analyzer=1;
SET allow_experimental_correlated_subqueries=1;

-- {echoOn}
SELECT number IN (SELECT * FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2)) FROM numbers(3);

SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2));

SELECT number IN (SELECT number, number FROM numbers(2)) FROM numbers(3); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS}

SELECT number IN (SELECT number IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2) WHERE number IN (SELECT * FROM numbers(1))) FROM numbers(3);

--{echoOff}
