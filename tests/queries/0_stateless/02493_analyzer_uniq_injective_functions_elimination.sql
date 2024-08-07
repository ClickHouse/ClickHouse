SET enable_analyzer = 1, optimize_injective_functions_inside_uniq = 1;

-- Simple test
EXPLAIN QUERY TREE SELECT uniqCombined(tuple('')) FROM numbers(1);
SELECT uniqCombined(tuple('')) FROM numbers(1);

-- Test with chain of injective functions
EXPLAIN QUERY TREE SELECT uniqCombined(tuple(materialize(tuple(number)))) FROM numbers(10);
SELECT uniqCombined(tuple(materialize(toString(number)))) FROM numbers(10);

-- No or partial optimization cases
EXPLAIN QUERY TREE SELECT uniq(abs(number)) FROM numbers(10); -- no elimination as `abs` is not injective
EXPLAIN QUERY TREE SELECT uniq(toString(abs(materialize(number)))) FROM numbers(10); -- only eliminate `toString`
EXPLAIN QUERY TREE SELECT uniq(tuple(number, 1)) FROM numbers(10); -- no elimination as `tuple` has multiple arguments
