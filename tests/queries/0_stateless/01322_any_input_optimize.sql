SET optimize_move_functions_out_of_any=1;
SET enable_debug_queries=1;
SELECT any(number + number * 2) FROM  numbers(3, 10);
ANALYZE SELECT any(number + number * 2) FROM  numbers(3, 10);
