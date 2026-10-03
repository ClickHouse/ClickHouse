SET allow_experimental_time_series_aggregate_functions = 1;

WITH
    [0, 3]::Array(UInt32) AS timestamps,
    [1, 2]::Array(Float32) AS values,
    arrayZip(timestamps, values) AS samples,
    timeSeriesDerivToGrid(3, 3, 1, 4)(timestamps, values) AS deriv_arrays,
    timeSeriesDerivToGrid(3, 3, 1, 4)(samples) AS deriv_pairs,
    timeSeriesPredictLinearToGrid(3, 3, 1, 4, 1)(timestamps, values) AS predict_arrays,
    timeSeriesPredictLinearToGrid(3, 3, 1, 4, 1)(samples) AS predict_pairs
SELECT
    toTypeName(deriv_arrays),
    toTypeName(deriv_pairs),
    toTypeName(predict_arrays),
    toTypeName(predict_pairs),
    abs(assumeNotNull(deriv_arrays[1]) - toFloat64(1) / 3) < 1e-15,
    abs(assumeNotNull(deriv_pairs[1]) - toFloat64(1) / 3) < 1e-15,
    abs(assumeNotNull(predict_arrays[1]) - toFloat64(7) / 3) < 1e-15,
    abs(assumeNotNull(predict_pairs[1]) - toFloat64(7) / 3) < 1e-15;

SELECT
    toTypeName(deriv),
    toTypeName(predict),
    abs(assumeNotNull(deriv[1]) - toFloat64(1) / 3) < 1e-15,
    abs(assumeNotNull(predict[1]) - toFloat64(7) / 3) < 1e-15
FROM
(
    SELECT
        timeSeriesDerivToGrid(3, 3, 1, 4)(timestamp, value) AS deriv,
        timeSeriesPredictLinearToGrid(3, 3, 1, 4, 1)(timestamp, value) AS predict
    FROM
    (
        SELECT
            toUInt32(number * 3) AS timestamp,
            toFloat32(number + 1) AS value
        FROM numbers(2)
    )
);