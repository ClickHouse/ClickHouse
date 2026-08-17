SELECT quantilePrometheusHistogram(0.9)(args.1, args.2 + number)
FROM (
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf], 
        [0.0, 10.0, 11.0, 12.0]
    )) AS args, number
    FROM numbers(10)
)
GROUP BY number
ORDER BY number;

SELECT quantilePrometheusHistogram(0.9)(toFloat32(args.1), args.2 + number) -- Float32 upper bound values
FROM (
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf], 
        [0.0, 10.0, 11.0, 12.0]
    )) AS args, number
    FROM numbers(10)
)
GROUP BY number
ORDER BY number;

SELECT quantilePrometheusHistogram(0.9)(args.1, args.2 + number) -- UInt cumulative histogram values
FROM (
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf], 
        [0, 10, 11, 12]
    )) AS args, number
    FROM numbers(10)
)
GROUP BY number
ORDER BY number;

SELECT quantilePrometheusHistogram(0.9)(args.1, args.2) -- return NaN if no inf bucket
FROM (
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0], 
        [0.0, 10.0, 11.0]
    )) AS args
);

SELECT quantilePrometheusHistogram(0.5)(+Inf, 10.0); -- return NaN if less than 2 buckets

SELECT quantilePrometheusHistogramArray(0.9)(args.1, [args.2, args.2 + 1])
FROM
(
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf],
        [0.0, 10.0, 11.0, 12.0]
    )) AS args
);

SELECT quantilePrometheusHistogramArray(0.9)(args.1, CAST([args.2, args.2 + 1], 'Array(Nullable(Float32))'))
FROM
(
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf],
        [0.0, 10.0, 11.0, 12.0]
    )) AS args
);

SELECT quantilePrometheusHistogramArray(0.5)(le, values)
FROM
(
    SELECT toFloat64(0) AS le, CAST([], 'Array(Nullable(Float64))') AS values
    UNION ALL
    SELECT inf AS le, CAST([2., 3.], 'Array(Nullable(Float64))') AS values
);

SELECT
    quantilePrometheusHistogramForEach(0.5)(
        arrayResize(CAST([], 'Array(Float64)'), length(values), le), values) AS old,
    quantilePrometheusHistogramArray(0.5)(le, values) AS new
FROM
(
    SELECT toFloat64(0) AS le, CAST([1., NULL], 'Array(Nullable(Float64))') AS values
    UNION ALL
    SELECT inf AS le, CAST([2.], 'Array(Nullable(Float64))') AS values
);

SELECT
    quantilePrometheusHistogramForEach(0.5)(
        arrayResize(CAST([], 'Array(Float64)'), length(values), le), values) AS old,
    quantilePrometheusHistogramArray(0.5)(le, values) AS new
FROM
(
    SELECT nan AS le, CAST([1., 2.], 'Array(Nullable(Float64))') AS values
);

SELECT quantilePrometheusHistogramArrayMerge(0.9)(state)
FROM
(
    SELECT part, quantilePrometheusHistogramArrayState(0.9)(le, values) AS state
    FROM
    (
        SELECT 1 AS part, 0.0 AS le, [0.0, 1.0] AS values
        UNION ALL
        SELECT 2 AS part, 0.5 AS le, [10.0, 11.0] AS values
        UNION ALL
        SELECT 3 AS part, 1.0 AS le, [11.0, 12.0] AS values
        UNION ALL
        SELECT 4 AS part, inf AS le, [12.0, 13.0] AS values
    )
    GROUP BY part
);

SELECT quantilePrometheusHistogram(0.2)(args.1, args.2) -- interpolate between minimum bucket upper bound and 0
FROM (
    SELECT arrayJoin(arrayZip(
        [0.5, 1.0, 2.0, +Inf], 
        [5.0, 10.0, 13.0, 15.0]
    )) AS args
);

SELECT quantilePrometheusHistogram(0.2)(args.1, args.2) -- do not interpolate if quantile position is in minimum bucket and minimum bucket upper bound is negative
FROM (
    SELECT arrayJoin(arrayZip(
        [-0.5, 0.0, 1.0, +Inf], 
        [5.0, 10.0, 13.0, 15.0]
    )) AS args
);

SELECT quantilesPrometheusHistogram(0, 0.1, 0.3, 0.5, 0.7, 0.9, 1)(args.1, args.2 + number)
FROM (
    SELECT arrayJoin(arrayZip(
        [0.0, 0.5, 1.0, +Inf], 
        [0.0, 10.0, 11.0, 12.0]
    )) AS args, number
    FROM numbers(10)
)
GROUP BY number
ORDER BY number;
