SET allow_experimental_analyzer = 0;

-- view() table function with an inner JOIN used inside an outer JOIN
-- used to trigger LOGICAL_ERROR "Inconsistent table names" because
-- CrossToInnerJoinVisitor descended into table function arguments
-- and tried to check the inner SELECT's tables against the outer query's metadata.

SELECT 1 FROM system.one AS t1
INNER JOIN view(
    SELECT dummy AS d2 FROM system.one
    INNER JOIN (SELECT 0 AS d2) AS a ON a.d2 = d2
) AS j ON j.d2 = t1.dummy;
