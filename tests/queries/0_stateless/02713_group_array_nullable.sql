SELECT groupArray(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT groupArrayLast(2)(number) FROM numbers(10) SETTINGS aggregate_functions_null_for_empty = 1;
