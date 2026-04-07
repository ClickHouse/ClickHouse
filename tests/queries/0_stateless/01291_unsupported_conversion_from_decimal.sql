SELECT toIntervalSecond(now64()); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(now64() AS IntervalSecond); -- { serverError CANNOT_CONVERT_TYPE }

SELECT toIntervalSecond(now64()); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(now64() AS IntervalSecond); -- { serverError CANNOT_CONVERT_TYPE }
