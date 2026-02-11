-- Verify that projections with positional GROUP BY arguments are rewritten
-- to use actual column names, so they survive DETACH/ATTACH (server restart).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=96357&sha=76567a89617ff50443418c3f4297fab8183d22d5&name_0=PR&name_1=Stress%20test%20%28amd_tsan%29

SET enable_positional_arguments_for_projections = 1;

-- Test 1: CREATE TABLE with positional GROUP BY in projection
DROP TABLE IF EXISTS test_proj_positional;

CREATE TABLE test_proj_positional
(
    `a` UInt64,
    `b` String,
    PROJECTION test_projection (SELECT b, a GROUP BY 1, 2)
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test_proj_positional VALUES (1, 'x'), (2, 'y');

DETACH TABLE test_proj_positional;
SET enable_positional_arguments_for_projections = 0;
ATTACH TABLE test_proj_positional;

SELECT * FROM test_proj_positional ORDER BY a;

DROP TABLE test_proj_positional;

-- Test 2: ALTER TABLE ADD PROJECTION with positional GROUP BY
SET enable_positional_arguments_for_projections = 1;

CREATE TABLE test_proj_positional
(
    `a` UInt64,
    `b` String
)
ENGINE = MergeTree
ORDER BY a;

ALTER TABLE test_proj_positional ADD PROJECTION test_projection (SELECT b, a GROUP BY 1, 2);

INSERT INTO test_proj_positional VALUES (3, 'z'), (4, 'w');

DETACH TABLE test_proj_positional;
SET enable_positional_arguments_for_projections = 0;
ATTACH TABLE test_proj_positional;

SELECT * FROM test_proj_positional ORDER BY a;

DROP TABLE test_proj_positional;
