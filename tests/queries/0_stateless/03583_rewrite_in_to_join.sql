SET enable_analyzer=1;
SET rewrite_in_to_join=1;
SET allow_experimental_correlated_subqueries=1;

-- {echoOn}
-- Check that with these settings the plan contains a join
SELECT explain FROM (
    EXPLAIN keep_logical_steps=1, description=0 SELECT number IN (SELECT * FROM numbers(2)) FROM numbers(3)
) WHERE explain ILIKE '%join%';

SELECT number IN (SELECT * FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2)) FROM numbers(3);

SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2));

SELECT number IN (SELECT number, number FROM numbers(2)) FROM numbers(3); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT}

SELECT number IN (SELECT number IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2) WHERE number IN (SELECT * FROM numbers(1))) FROM numbers(3);

-- NOT IN
SELECT number NOT IN (SELECT * FROM numbers(2)) FROM numbers(3);

SELECT * FROM numbers(3) WHERE number NOT IN (SELECT number FROM numbers(2));

SELECT number NOT IN (SELECT number, number FROM numbers(2)) FROM numbers(3); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT}

SELECT number NOT IN (SELECT number IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number NOT IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number NOT IN (SELECT number NOT IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2) WHERE number NOT IN (SELECT * FROM numbers(1))) FROM numbers(3);


EXPLAIN keep_logical_steps=1, description=0
SELECT *
FROM numbers(8)
WHERE number IN (select number from numbers(5));

-- Same subquery as CTE
EXPLAIN keep_logical_steps=1, description=0
WITH
    t as (select number from numbers(5))
SELECT *
FROM numbers(8)
WHERE number IN t;

WITH
    t as (select number from numbers(5))
SELECT *
FROM numbers(8)
WHERE number IN t;

-- Tuple
SELECT *
FROM numbers(8)
WHERE (number+1, number+2) IN (select number, number+1 from numbers(5));

-- Tuple and CTE
WITH
    t as (select number, number+1 from numbers(5))
SELECT *
FROM numbers(8)
WHERE (number+1, number+2) in (t);

-- Mismatching number of elements 
SELECT *
FROM numbers(8)
WHERE (number+1, number+2, number+3) IN (select number, number+1 from numbers(5)); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT}

WITH
    t as (select number, number+1 from numbers(5))
SELECT *
FROM numbers(8)
WHERE (number+1, number+2, number+3) IN (t); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS, ILLEGAL_TYPE_OF_ARGUMENT}

--{echoOff}
