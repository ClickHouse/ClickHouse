SELECT toIntervalSecond(now64()); -- { serverError 70 }
SELECT CAST(now64() AS IntervalSecond); -- { serverError 70 }

SELECT toIntervalSecond(now64()); -- { serverError 70 }
SELECT CAST(now64() AS IntervalSecond); -- { serverError 70 }
