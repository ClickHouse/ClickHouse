SELECT '--- Basic Examples ---';
SELECT arrayAutocorrelation([1, 2, 3, 4, 5]);
SELECT arrayAutocorrelation([10, 20, 10]);

SELECT '--- Constant & Small Arrays ---';
SELECT arrayAutocorrelation([5, 5, 5]);
SELECT arrayAutocorrelation([5, 5]);
SELECT arrayAutocorrelation([5]);
SELECT arrayAutocorrelation([]);

SELECT '--- Max Lag Argument ---';
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 2);
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 0);
SELECT arrayAutocorrelation([1, 2, 3, 4, 5], 100);

SELECT '--- Type Dispatch Coverage ---';
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt8)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt16)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt64)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt128)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(UInt256)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int8)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int16)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int64)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int128)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Int256)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Float32)'));
SELECT arrayAutocorrelation(CAST([1, 2, 3], 'Array(Float64)'));

SELECT '--- Table Execution ---';
DROP TABLE IF EXISTS test_autocorr;
CREATE TABLE test_autocorr (
    id UInt64,
    data Array(Float64),
    lag UInt64
) ENGINE = Memory;

INSERT INTO test_autocorr VALUES (1, [1, 2, 3, 4, 5], 2), (2, [10, 20, 10], 5), (3, [5, 5, 5], 2);

SELECT id, arrayAutocorrelation(data) FROM test_autocorr ORDER BY id;
SELECT id, arrayAutocorrelation(data, lag) FROM test_autocorr ORDER BY id;

DROP TABLE test_autocorr;

SELECT '--- Negative Tests (Expect Errors) ---';
SELECT arrayAutocorrelation(['a', 'b']); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT arrayAutocorrelation([NULL, 1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }