SELECT number FROM numbers(10) WHERE has([number % 3, number % 5], number % 2) ORDER BY number;
SELECT '-- IN --';
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 1;
SELECT number FROM numbers(10) WHERE number % 2 IN [number % 3, number % 5] ORDER BY number SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT '-- MORE CASES --';

-- { echoOn }

SELECT null in [number % 3, number % 5] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }
SELECT null in [number % 3, number % 5, null] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }
SELECT 5 in [number % 3, number % 5, null] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }

SELECT (1, 2) in [number % 3, number % 5] FROM numbers(2); -- { serverError NO_COMMON_TYPE }
SELECT (1, 2) in (SELECT [0, 0] UNION ALL SELECT [1, 1]); -- { serverError TYPE_MISMATCH }

SELECT (1, 2) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 2) in (SELECT (0, 0)), (1, 2) in (SELECT (1, 1));

SELECT (1, 1) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 1) in (SELECT (0, 0)), (1, 1) in (SELECT (1, 1));

SELECT (1, null) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int))), (1, null) in (SELECT (1, 1::Nullable(Int)));

SELECT (1, null) in [(number % 3, number % 5), (1, null)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int)) UNION ALL SELECT (1, null)), (1, null) in (SELECT (1, 1::Nullable(Int)) UNION ALL SELECT (1, null));

SELECT 'ANOTHER SETTING';

set transform_null_in = 1;

SELECT null in [number % 3, number % 5] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }
SELECT null in [number % 3, number % 5, null] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }
SELECT 5 in [number % 3, number % 5, null] FROM numbers(2); -- { serverError UNSUPPORTED_METHOD }

SELECT (1, 2) in [number % 3, number % 5] FROM numbers(2); -- { serverError NO_COMMON_TYPE }
SELECT (1, 2) in (SELECT [0, 0] UNION ALL SELECT [1, 1]); -- { serverError TYPE_MISMATCH }

SELECT (1, 2) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 2) in (SELECT (0, 0)), (1, 2) in (SELECT (1, 1));

SELECT (1, 1) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, 1) in (SELECT (0, 0)), (1, 1) in (SELECT (1, 1));

SELECT (1, null) in [(number % 3, number % 5)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int))), (1, null) in (SELECT (1, 1::Nullable(Int)));

SELECT (1, null) in [(number % 3, number % 5), (1, null)] FROM numbers(2);
SELECT (1, null) in (SELECT (0, 0::Nullable(Int)) UNION ALL SELECT (1, null)), (1, null) in (SELECT (1, 1::Nullable(Int)) UNION ALL SELECT (1, null));

