SET enable_analyzer = 1;

WITH (y + 1) AS x
SELECT if(1 = 1, arrayMap(y -> x, [10]), x);

WITH (y + 1) AS x
SELECT if(1 = 0, x, arrayMap(y -> x, [10]));

WITH (z + 1) AS y, y AS x
SELECT if(1 = 1, arrayMap(z -> x, [10]), x);

WITH (y + 1) AS x
SELECT multiIf(1 = 1, arrayMap(y -> x, [10]), 1 = 0, x, [0]);

WITH (SELECT sum(throwIf(number = 0, 'boom')) FROM numbers(1)) AS bad_x
SELECT if(1 = 1, bad_x + 1, bad_x + 2); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

WITH bad_cte AS (SELECT unknown_col)
SELECT if(1 = 1, (SELECT * FROM bad_cte), (SELECT * FROM bad_cte)); -- { serverError UNKNOWN_IDENTIFIER }
