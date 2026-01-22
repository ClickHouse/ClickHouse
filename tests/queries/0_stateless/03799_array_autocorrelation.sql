SELECT '--- Basic Floats ---';
SELECT arrayAutocorrelation([10.0, 20.0, 30.0]);

SELECT '--- Integers (Auto-converted) ---';
SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);

SELECT '--- Constant Arrays ---';
SELECT arrayAutocorrelation([5, 5, 5]);

SELECT '--- Tiny Arrays (Edge Case) ---';
SELECT arrayAutocorrelation([1]);
SELECT arrayAutocorrelation(CAST([], 'Array(Float64)'));

SELECT '--- Input Validation (Should Fail) ---';
SELECT arrayAutocorrelation('not an array'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }