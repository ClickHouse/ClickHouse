-- Force a three-point dense destination to merge a four-point sparse source.
-- arrayReduce fixes the merge order rather than relying on UNION ALL scheduling.
DROP TABLE IF EXISTS histogram_grid_merge_states;
CREATE TABLE histogram_grid_merge_states ENGINE = Memory AS
SELECT part, quantilePrometheusHistogramArrayState(0.5)(le, values) AS state
FROM
(
    SELECT 0 AS part, arrayJoin([1., inf]) AS le,
        CAST(if(isFinite(le), [1., 1., NULL], [2., 2., NULL]), 'Array(Nullable(Float64))') AS values
    UNION ALL
    SELECT 1 AS part, arrayJoin([1., inf]) AS le,
        CAST(if(isFinite(le), [NULL, NULL, NULL, 1.], [NULL, NULL, NULL, 2.]), 'Array(Nullable(Float64))') AS values
)
GROUP BY part;

WITH
    (SELECT state FROM histogram_grid_merge_states WHERE part = 0) AS shorter,
    (SELECT state FROM histogram_grid_merge_states WHERE part = 1) AS longer
SELECT
    arrayMap(x -> if(isNaN(x), -1., x), arrayReduce('quantilePrometheusHistogramArrayMerge(0.5)', [shorter, longer])),
    arrayMap(x -> if(isNaN(x), -1., x), arrayReduce('quantilePrometheusHistogramArrayMerge(0.5)', [longer, shorter]));
DROP TABLE histogram_grid_merge_states;

-- CTAS must be able to reparse state types with integer and Decimal parameters.
DROP TABLE IF EXISTS histogram_grid_typed_states;
CREATE TABLE histogram_grid_typed_states ENGINE = Memory AS
SELECT
    quantilePrometheusHistogramArrayState(1)(pair.1, [pair.2]) AS integer_state,
    quantilePrometheusHistogramArrayState(toDecimal64('0.25', 2))(pair.1, [pair.2]) AS decimal_state
FROM (SELECT arrayJoin([(1., 1.), (inf, 2.)]) AS pair);

SELECT finalizeAggregation(integer_state), finalizeAggregation(decimal_state)
FROM histogram_grid_typed_states;
SELECT
    quantilePrometheusHistogramArrayMerge(1)(integer_state),
    quantilePrometheusHistogramArrayMerge(toDecimal64('0.25', 2))(decimal_state)
FROM histogram_grid_typed_states;
DROP TABLE histogram_grid_typed_states;
