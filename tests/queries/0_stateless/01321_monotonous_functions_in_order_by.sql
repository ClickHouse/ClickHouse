SET enable_debug_queries = 1;
SET optimize_monotonous_functions_in_order_by = 1;

SELECT number FROM numbers(10) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(10) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(10) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(10) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(10) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(10) ORDER BY toFloat32(abs(number));

SET optimize_monotonous_functions_in_order_by = 0;

SELECT number FROM numbers(10) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(10) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(10) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(10) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(10) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(10) ORDER BY toFloat32(abs(number));
