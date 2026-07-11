-- Regression test: CROSS JOIN combined with INNER JOIN USING caused
-- a logical error in CollectSourceColumnsVisitor because CROSS_JOIN
-- was not handled as a valid column source node type.
-- The bug is triggered when analyzer_compatibility_join_using_top_level_identifier
-- resolves a USING column from the SELECT projection with a CROSS_JOIN as
-- left table expression, assigning the CrossJoinNode as the column source.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=1aefdf9c553447757c0daa4a6d48fa875173b7ee&name_0=MasterCI&name_1=BuzzHouse%20%28amd_ubsan%29

SET enable_analyzer = 1;

SELECT 1 AS c0
FROM (SELECT 1 AS c0) AS t0
CROSS JOIN (SELECT 2 AS b) AS t1
INNER JOIN (SELECT 1 AS c0) AS t2 USING (c0)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1;

-- Previously caused a logical error; now correctly returns UNKNOWN_IDENTIFIER
-- because c0 is not in the left table expressions and the fix prevents
-- assigning CROSS_JOIN node as the column source from the projection alias.
SELECT 1 AS c0
FROM (SELECT 1 AS a) AS t0
CROSS JOIN (SELECT 2 AS b) AS t1
INNER JOIN (SELECT 1 AS c0) AS t2 USING (c0)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError UNKNOWN_IDENTIFIER }

SELECT 1 AS c0
FROM (SELECT 1 AS a) AS t0
CROSS JOIN (SELECT 2 AS b) AS t1
CROSS JOIN (SELECT 3 AS c) AS t2
INNER JOIN (SELECT 1 AS c0) AS t3 USING (c0)
SETTINGS analyzer_compatibility_join_using_top_level_identifier = 1; -- { serverError UNKNOWN_IDENTIFIER }
