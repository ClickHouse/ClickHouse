SELECT SUM(number)
FROM
(
    SELECT 10 AS number
)
GROUP BY number
HAVING 1 AND sin(SUMOrNull(number))
SETTINGS enable_optimize_predicate_expression = 0;

select '#45218';

SELECT SUM(number)
FROM
(
    SELECT 10 AS number
)
GROUP BY cos(min2(number, number) % number) - number
HAVING ((-sign(-233841197)) IS NOT NULL) AND sin(lcm(SUM(number), SUM(number)) >= ('372497213' IS NOT NULL))
SETTINGS aggregate_functions_null_for_empty = 1, enable_optimize_predicate_expression = 0;

select '=';
