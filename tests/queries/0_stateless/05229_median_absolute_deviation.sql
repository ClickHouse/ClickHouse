SELECT mad(x), medianAbsoluteDeviation(x)
FROM (SELECT arrayJoin([0, 10, 20, 30]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([0, 10]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([0, 10, 20]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([-10, -5, 0, 5, 10]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([7]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([4, 4, 4, 4]) AS x);

SELECT mad(toUInt8(number)), mad(toUInt16(number)), mad(toUInt32(number)), mad(toUInt64(number)),
       mad(toInt8(number)), mad(toInt16(number)), mad(toInt32(number)), mad(toInt64(number)),
       mad(toFloat32(number)), mad(toFloat64(number))
FROM numbers(4);

SELECT mad(x)
FROM (SELECT arrayJoin([CAST(9007199254740992 AS Int64), CAST(9007199254740993 AS Int64)]) AS x);

SELECT isFinite(mad(x))
FROM (SELECT arrayJoin([CAST(-9223372036854775807 AS Int64), CAST(9223372036854775807 AS Int64)]) AS x);

SELECT isFinite(mad(x))
FROM (SELECT arrayJoin([toFloat64(-1.7976931348623157e308), toFloat64(1.7976931348623157e308)]) AS x);

SELECT mad(x)
FROM (SELECT arrayJoin([toFloat64(1), nan, toFloat64(3)]) AS x);

SELECT isNaN(mad(x))
FROM (SELECT nan AS x FROM numbers(2));

SELECT isNaN(mad(number))
FROM numbers(0);

SELECT mad(x)
FROM
(
    SELECT arrayJoin([
        CAST(0 AS Nullable(Int32)),
        CAST(10 AS Nullable(Int32)),
        CAST(NULL AS Nullable(Int32)),
        CAST(20 AS Nullable(Int32)),
        CAST(30 AS Nullable(Int32))]) AS x
);

SELECT isNull(madOrNull(x))
FROM (SELECT CAST(NULL AS Nullable(Int32)) AS x);

SELECT madIf(number, number % 2 = 0)
FROM numbers(6);

SELECT madDistinct(x)
FROM (SELECT arrayJoin([0, 0, 10, 10]) AS x);

SELECT madArray([0, 10, 20, 30]);

SELECT madMerge(state)
FROM (SELECT madState(number) AS state FROM numbers(4));

SELECT finalizeAggregation(state), finalizeAggregation(state)
FROM (SELECT madState(number) AS state FROM numbers(4));

SELECT key, mad(x)
FROM
(
    SELECT number % 2 AS key, number AS x
    FROM numbers(6)
)
GROUP BY key
ORDER BY key;

SELECT mad(x)
FROM (SELECT arrayJoin([1.0, inf]) AS x); -- { serverError BAD_ARGUMENTS }
