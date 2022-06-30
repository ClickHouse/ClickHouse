SELECT test_function_with_parameter('test')(1, 2); --{serverError 53}
SELECT test_function_with_parameter(2, 2)(1, 2); --{serverError 36}
SELECT test_function_with_parameter(1, 2); --{serverError 36}

SELECT test_function_with_parameter(2)(1, 2);
SELECT test_function_with_parameter('2')(1, 2);
