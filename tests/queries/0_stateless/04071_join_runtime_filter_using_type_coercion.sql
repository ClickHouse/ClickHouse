-- Regression test: `mergeFilterIntoJoinCondition` must not push a WHERE
-- equality into a JOIN condition when an input column's type was changed by
-- USING coercion. Previously, `getExpressionSide` ignored inputs not in
-- either allowed set, misclassifying the expression and causing a
-- LOGICAL_ERROR: "Unexpected return type from round. Expected UInt16. Got Const(UInt8)".

SELECT 1 FROM (SELECT 1 AS x, 1 AS y) AS a INNER JOIN (SELECT 1023 AS y) AS b USING (y) WHERE round(*) = b.y;
