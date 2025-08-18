SET enable_analyzer=1;
SET rewrite_in_to_join=1;
SET allow_experimental_correlated_subqueries=1;

-- {echoOn}
-- Check that with these settings the plan contains a join
SELECT explain FROM (
    EXPLAIN SELECT number IN (SELECT * FROM numbers(2)) FROM numbers(3)
) WHERE explain ILIKE '%join%';

SELECT number IN (SELECT * FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2)) FROM numbers(3);

SELECT * FROM numbers(3) WHERE number IN (SELECT number FROM numbers(2));

SELECT number IN (SELECT number, number FROM numbers(2)) FROM numbers(3); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS}

SELECT number IN (SELECT number IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2) WHERE number IN (SELECT * FROM numbers(1))) FROM numbers(3);

-- NOT IN
SELECT number NOT IN (SELECT * FROM numbers(2)) FROM numbers(3);

SELECT * FROM numbers(3) WHERE number NOT IN (SELECT number FROM numbers(2));

SELECT number NOT IN (SELECT number, number FROM numbers(2)) FROM numbers(3); -- {serverError NUMBER_OF_COLUMNS_DOESNT_MATCH,BAD_ARGUMENTS}

SELECT number NOT IN (SELECT number IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number NOT IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number NOT IN (SELECT number NOT IN (SELECT * FROM numbers(1)) FROM numbers(2)) FROM numbers(3);

SELECT number IN (SELECT number FROM numbers(2) WHERE number NOT IN (SELECT * FROM numbers(1))) FROM numbers(3);

--{echoOff}
