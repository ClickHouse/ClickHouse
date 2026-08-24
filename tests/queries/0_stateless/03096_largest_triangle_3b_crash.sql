SELECT largestTriangleThreeBuckets(1)(1, nan);
SELECT finalizeAggregation(
    CAST(
        substring(s, 1, 1) || char(0) || substring(s, 2) AS AggregateFunction(largestTriangleThreeBuckets(2), Float64, Float64)
    )
)
FROM (
    SELECT CAST(
        groupArrayState(number::Float64) AS String) AS s
    FROM numbers(2)
); -- { serverError INCORRECT_DATA }
