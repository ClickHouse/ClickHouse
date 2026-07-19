-- Test for support of non-constant arguments in random distribution functions.
-- https://github.com/ClickHouse/ClickHouse/issues/59302
-- Only deterministic properties of the results are checked.

-- Both arguments are non-constant: the result is always inside [min, max].
SELECT r >= number AND r <= number + 1 FROM (SELECT number, randUniform(number, number + 1) AS r FROM numbers(5));

-- Mix of constant and non-constant arguments.
SELECT r >= 0 AND r <= number + 1 FROM (SELECT number, randUniform(0, number + 1) AS r FROM numbers(5));

-- Bernoulli with probability 0 always returns 0 and with probability 1 always returns 1.
SELECT randBernoulli(number % 2) = number % 2 FROM numbers(6);

-- Invalid parameters in a later row must throw (the first rows have valid parameters).
SELECT randUniform(number, 1) FROM numbers(5); -- { serverError BAD_ARGUMENTS }
SELECT randBernoulli(number) FROM numbers(3); -- { serverError BAD_ARGUMENTS }
