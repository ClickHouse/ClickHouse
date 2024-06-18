SELECT number FROM numbers(10) WHERE has([number % 3, number % 5], number % 2) ORDER BY number;
SELECT '-- IN --';
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 1;
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }
