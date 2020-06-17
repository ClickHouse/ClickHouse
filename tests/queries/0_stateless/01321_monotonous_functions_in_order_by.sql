SET enable_debug_queries = 1;
SET optimize_monotonous_functions_in_order_by = 1;

SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
SELECT number FROM numbers(3) ORDER BY -number;
SELECT number FROM numbers(3) ORDER BY -number DESC;
SELECT number FROM numbers(3) ORDER BY exp(number);
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(3) ORDER BY -number;
analyze SELECT number FROM numbers(3) ORDER BY -number DESC;
analyze SELECT number FROM numbers(3) ORDER BY exp(number);

SET optimize_monotonous_functions_in_order_by = 0;

SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
SELECT number FROM numbers(3) ORDER BY -number;
SELECT number FROM numbers(3) ORDER BY -number DESC;
SELECT number FROM numbers(3) ORDER BY exp(number);
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(toFloat64(number));
analyze SELECT number FROM numbers(3) ORDER BY abs(toFloat32(number));
analyze SELECT number FROM numbers(3) ORDER BY toFloat32(abs(number));
analyze SELECT number FROM numbers(3) ORDER BY -number;
analyze SELECT number FROM numbers(3) ORDER BY -number DESC;
analyze SELECT number FROM numbers(3) ORDER BY exp(number);
-- TODO: exp() should be monotonous function
