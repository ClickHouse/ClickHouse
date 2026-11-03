SET enable_analyzer = 1;

WITH 10 AS x
SELECT if(1 = 1, x + 1, (unknown_identifier + 1) AS x);

WITH (SELECT 10) AS x
SELECT if(1 = 1, x + 1, (unknown_identifier + 1) AS x);

WITH (SELECT 10) AS x
SELECT if(1 = 0, (unknown_identifier + 1) AS x, x + 2);

WITH 10 AS x
SELECT multiIf(1 = 1, x + 1, 1 = 0, (unknown_identifier + 1) AS x, x + 2);

WITH (SELECT sum(throwIf(number = 0, 'boom')) FROM numbers(1)) AS bad_x
SELECT if(1 = 1, bad_x + 1, bad_x + 2); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
