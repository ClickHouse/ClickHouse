SELECT arrayWithConstant(3, number) FROM numbers(10);
SELECT arrayWithConstant(number, 'Hello') FROM numbers(10);
SELECT arrayWithConstant(number % 3, number % 2 ? 'Hello' : NULL) FROM numbers(10);
SELECT arrayWithConstant(number, []) FROM numbers(10);
