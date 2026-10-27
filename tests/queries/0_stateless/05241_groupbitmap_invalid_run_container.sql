-- Malformed groupBitmap states (a run container with n_runs = 0) must be rejected at deserialization, not crash on a set operation.
SELECT CAST(unhex('010b3b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt32)'); -- { serverError INCORRECT_DATA }

SELECT groupBitmapMerge(s) FROM
(
    SELECT CAST(unhex('010b3b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt32)') AS s
    UNION ALL
    SELECT CAST(unhex('010b3b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt32)') AS s
); -- { serverError INCORRECT_DATA }

SELECT bitmapCardinality(bitmapOr(
    CAST(unhex('010b3b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt32)'),
    CAST(unhex('010b3b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt32)')
)); -- { serverError INCORRECT_DATA }

SELECT groupBitmapMerge(s) FROM
(
    SELECT CAST(unhex('01170100000000000000000000003b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt64)') AS s
    UNION ALL
    SELECT CAST(unhex('01170100000000000000000000003b30000001000000000000'), 'AggregateFunction(groupBitmap, UInt64)') AS s
); -- { serverError INCORRECT_DATA }

SELECT groupBitmapMerge(s) FROM (SELECT groupBitmapState(number::UInt32) AS s FROM numbers(1000));
SELECT groupBitmapMerge(s) FROM (SELECT groupBitmapState(number::UInt64) AS s FROM numbers(1000));
