-- A bare scalar column on the right of IN with a tuple LHS is a one-element set and must
-- use the direct row-wise comparison, like the function-node analog
-- `(1, 2) IN (materialize(NULL))` in 04827. Wrapping it in tuple() sent it through the
-- tuple-set rewrite, where the least supertype of the tuple LHS and a NULL column becomes
-- `Nullable(Tuple(...))` and the query failed with ILLEGAL_COLUMN.

-- { echoOn }

SELECT (1, 2) IN (rhs), (1, 2) NOT IN (rhs) FROM (SELECT materialize(NULL) AS rhs);
SELECT nullIn((1, 2), rhs), notNullIn((1, 2), rhs) FROM (SELECT materialize(NULL) AS rhs);
SELECT (1, 2) IN (rhs), (1, 2) NOT IN (rhs) FROM (SELECT materialize(NULL) AS rhs) SETTINGS transform_null_in = 1;
SELECT (toNullable(1), 2) IN (rhs), (toNullable(1), 2) NOT IN (rhs), toTypeName((toNullable(1), 2) IN (rhs)) FROM (SELECT materialize(NULL) AS rhs);
SELECT (x, y) IN (rhs) FROM (SELECT 1 AS x, 2 AS y, materialize(NULL) AS rhs);

-- A non-NULL scalar column is rejected like the function-node analog.
SELECT (1, 2) IN (rhs) FROM (SELECT materialize(5) AS rhs); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A tuple-typed column stays a single set element.
SELECT (1, 2) IN (rhs), (2, 3) IN (rhs) FROM (SELECT materialize((1, 2)) AS rhs);
