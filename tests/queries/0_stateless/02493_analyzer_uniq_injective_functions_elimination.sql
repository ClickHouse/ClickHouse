EXPLAIN QUERY TREE SELECT uniqCombined(tuple('')) FROM numbers(1) SETTINGS allow_experimental_analyzer = 1, optimize_injective_functions_inside_uniq = 1;

SELECT uniqCombined(tuple('')) FROM numbers(1) SETTINGS allow_experimental_analyzer = 1, optimize_injective_functions_inside_uniq = 1;

EXPLAIN QUERY TREE SELECT uniqCombined(tuple(materialize(tuple(number)))) FROM numbers(10) SETTINGS allow_experimental_analyzer = 1, optimize_injective_functions_inside_uniq = 1;

SELECT uniqCombined(tuple(materialize(tuple(number)))) FROM numbers(10) SETTINGS allow_experimental_analyzer = 1, optimize_injective_functions_inside_uniq = 1;