SET function_range_max_elements_in_block = 10;
SELECT range(number % 3) FROM numbers(10);
SELECT range(number % 3) FROM numbers(11);
SELECT range(number % 3) FROM numbers(12); -- { serverError ARGUMENT_OUT_OF_BOUND }

SET function_range_max_elements_in_block = 12;
SELECT range(number % 3) FROM numbers(12);
