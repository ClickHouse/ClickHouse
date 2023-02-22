SELECT max(100-number), min(100-number) FROM numbers(2) SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1;
SELECT max(100-number), min(100-number) FROM numbers(2)  SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 1, allow_experimental_analyzer = 1;
