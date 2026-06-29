-- Reject unsupported `WITH CLUSTER` key types upfront with a clear
-- `BAD_ARGUMENTS`, instead of letting them reach the transform and fail
-- with an internal `NOT_IMPLEMENTED` from `getFloat64` on a non-numeric
-- column.

-- 2D tuple with String elements.
SELECT count() FROM VALUES('x UInt64, y UInt64', (1, 2))
GROUP BY (toString(x), toString(y)) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

-- 2D tuple with mixed (numeric + String).
SELECT count() FROM VALUES('x UInt64, y UInt64', (1, 2))
GROUP BY (x, toString(y)) WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

-- 1D non-scalar (Array key).
SELECT count() FROM (SELECT [1, 2] AS a)
GROUP BY a WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
