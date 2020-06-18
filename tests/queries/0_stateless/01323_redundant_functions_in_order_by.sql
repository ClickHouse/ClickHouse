set enable_debug_queries = 1;
set optimize_redundant_functions_in_order_by = 1;

SELECT number as x FROM numbers(3) ORDER BY x, exp(x);
SELECT number as x FROM numbers(3) ORDER BY exp(x), x;
analyze SELECT number as x FROM numbers(3) ORDER BY x, exp(x);
analyze SELECT number as x FROM numbers(3) ORDER BY exp(x), x;

set optimize_redundant_functions_in_order_by = 0;

SELECT number as x FROM numbers(3) ORDER BY x, exp(x);
SELECT number as x FROM numbers(3) ORDER BY exp(x), x;
analyze SELECT number as x FROM numbers(3) ORDER BY x, exp(x);
analyze SELECT number as x FROM numbers(3) ORDER BY exp(x), x;
