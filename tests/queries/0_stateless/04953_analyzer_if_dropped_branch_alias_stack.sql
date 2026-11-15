SET enable_analyzer = 1;

WITH (SELECT sum(throwIf(number = 0, 'boom')) FROM numbers(1)) AS bad_x
SELECT if(1 = 1, bad_x + 1, bad_x + 2); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

WITH bad_cte AS (SELECT unknown_col)
SELECT if(1 = 1, (SELECT * FROM bad_cte), (SELECT * FROM bad_cte)); -- { serverError UNKNOWN_IDENTIFIER }
