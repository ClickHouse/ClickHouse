SET aggregate_functions_null_for_empty = 1;
	
SELECT groupArray(1);
SELECT groupArray(number) FROM numbers(10);
SELECT groupArrayLast(2)(number) FROM numbers(10);
