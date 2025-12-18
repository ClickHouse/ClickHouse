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
