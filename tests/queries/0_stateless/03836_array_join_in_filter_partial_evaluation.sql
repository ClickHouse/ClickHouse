-- arrayJoin in filter should not cause exception during partial evaluation
-- in convertOuterJoinToInnerJoin optimization.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a1e0dff89e0b4aa8c21455db0bda2ca751f3387e&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_ubsan%29

SELECT *
FROM (SELECT 1 AS a) AS l
LEFT JOIN (SELECT 2 AS a) AS r ON l.a = r.a
WHERE arrayJoin([1, 2, 3]) = 1
SETTINGS join_use_nulls = 1;
