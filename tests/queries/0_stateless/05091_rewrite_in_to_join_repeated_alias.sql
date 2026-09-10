-- The `IN (subquery)` to join rewrite renames the subquery's projection to a name of its own, so that
-- it cannot collide with the outer scope. Drawing that name at random made two identical `IN`
-- expressions differ after the rewrite, and the analyzer - which accepts an alias repeated over
-- identical expressions - rejected such a query with `MULTIPLE_EXPRESSIONS_FOR_ALIAS`.

SET rewrite_in_to_join = 1;
SET allow_experimental_correlated_subqueries = 1;

SELECT 'the same aliased IN twice in the projection';
SELECT dummy IN (SELECT 1) AS ie, dummy IN (SELECT 1) AS ie FROM system.one;
SELECT dummy NOT IN (SELECT 1) AS ie, dummy NOT IN (SELECT 1) AS ie FROM system.one;

SELECT 'and nested in a condition';
SELECT if(1 = 1, if(dummy IN (SELECT 1) AS ie, 11, 22), if(dummy IN (SELECT 1) AS ie, 11, 22)) FROM system.one;

SELECT 'in a WHERE next to itself';
SELECT number FROM numbers(3) WHERE (number IN (SELECT 1) AS ie) OR (number IN (SELECT 1) AS ie) ORDER BY number;

SELECT 'two different subqueries under different aliases';
SELECT number IN (SELECT 1) AS one, number IN (SELECT 2) AS two FROM numbers(3) ORDER BY number;

-- An alias really used for two different expressions is still rejected.
SELECT 'a repeated alias over different expressions';
SELECT dummy IN (SELECT 1) AS ie, dummy IN (SELECT 2) AS ie FROM system.one; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
