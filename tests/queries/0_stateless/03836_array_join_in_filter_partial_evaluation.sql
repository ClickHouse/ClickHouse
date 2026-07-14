-- arrayJoin in filter should not cause exception during partial evaluation
-- in tryConvertAnyOuterJoinToInnerJoin optimization.
-- The bug only triggers with ANY LEFT/RIGHT JOIN (not ALL), because the
-- tryConvertAnyOuterJoinToInnerJoin code path calls filterResultForNotMatchedRows
-- which evaluates the filter with input_rows_count=1, and arrayJoin expands
-- a constant array into multiple rows, breaking the row count invariant.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a1e0dff89e0b4aa8c21455db0bda2ca751f3387e&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_ubsan%29

SELECT *
FROM (SELECT 1 AS a) AS l
ANY LEFT JOIN (SELECT 2 AS a) AS r ON l.a = r.a
WHERE arrayJoin([1, 2, 3]) = 1
SETTINGS join_use_nulls = 1;
