SET enable_debug_queries = 1;
SET optimize_move_functions_out_of_any = 1;

ANALYZE SELECT any(number + number * 2) FROM numbers(1, 2);
SELECT any(number + number * 2) FROM numbers(1, 2);

ANALYZE SELECT anyLast(number + number * 2) FROM numbers(1, 2);
SELECT anyLast(number + number * 2) FROM numbers(1, 2);

ANALYZE WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);
WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);

ANALYZE SELECT anyLast(number * 3) AS x, x FROM numbers(1, 2);
SELECT anyLast(number * 3) AS x, x FROM numbers(1, 2);

SELECT any(anyLast(number)) FROM numbers(1); -- { serverError 184 }

SET optimize_move_functions_out_of_any = 0;

ANALYZE SELECT any(number + number * 2) FROM numbers(1, 2);
SELECT any(number + number * 2) FROM numbers(1, 2);

ANALYZE SELECT anyLast(number + number * 2) FROM numbers(1, 2);
SELECT anyLast(number + number * 2) FROM numbers(1, 2);

ANALYZE WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);
WITH any(number * 3) AS x SELECT x FROM numbers(1, 2);

ANALYZE SELECT anyLast(number * 3) AS x, x FROM numbers(1, 2);
SELECT anyLast(number * 3) AS x, x FROM numbers(1, 2);

SELECT any(anyLast(number)) FROM numbers(1); -- { serverError 184 }
