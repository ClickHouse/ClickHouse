-- `estimateCompressionRatioIf` must only estimate the rows where the condition holds,
-- in every query shape: without keys, in a window, and in a single-key block of `GROUP BY`.
-- Each check compares against the same aggregation with the condition moved to `WHERE`.

SELECT 'no keys';
SELECT estimateCompressionRatioIf('NONE')(number, number < 10) FROM numbers(1000);
SELECT estimateCompressionRatio('NONE')(number) FROM numbers(1000) WHERE number < 10;

SELECT 'window';
SELECT DISTINCT estimateCompressionRatioIf('NONE')(number, number < 10) OVER () FROM numbers(1000);

SELECT 'single-key block';
SELECT k, estimateCompressionRatioIf('NONE')(number, number < 10) FROM numbers(1000) GROUP BY intDiv(number, 10000) AS k;

SELECT 'all rows match';
SELECT estimateCompressionRatioIf('NONE')(number, number < 10) = estimateCompressionRatio('NONE')(number) FROM numbers(10);

SELECT 'no rows match';
SELECT estimateCompressionRatioIf('NONE')(number, 0) FROM numbers(1000);
SELECT estimateCompressionRatio('NONE')(number) FROM numbers(1000) WHERE 0;

SELECT 'String';
SELECT estimateCompressionRatioIf('NONE')(toString(number), number % 7 = 3) = (SELECT estimateCompressionRatio('NONE')(toString(number)) FROM numbers(1000) WHERE number % 7 = 3) FROM numbers(1000);

SELECT 'Nullable argument';
-- `optimize_rewrite_aggregate_function_with_if` would turn `estimateCompressionRatio(if(c, x, NULL))` into `estimateCompressionRatioIf(x, c)`.
SET optimize_rewrite_aggregate_function_with_if = 0;
SELECT estimateCompressionRatioIf('NONE')(if(number < 20, number, NULL), number % 2 = 0) = (SELECT estimateCompressionRatio('NONE')(if(number < 20, number, NULL)) FROM numbers(1000) WHERE number % 2 = 0) FROM numbers(1000);
SET optimize_rewrite_aggregate_function_with_if = 1;

SELECT 'IfOrNullIf';
SELECT estimateCompressionRatioIfOrNullIf('NONE')(number, number < 10, 1) FROM numbers(1000);
SELECT estimateCompressionRatioIfOrNullIf('NONE')(number, number < 10, 0) FROM numbers(1000);
