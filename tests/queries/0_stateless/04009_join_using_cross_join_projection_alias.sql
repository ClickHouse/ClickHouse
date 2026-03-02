-- Fix: LOGICAL_ERROR exception when CROSS JOIN is left table expression of JOIN USING
-- with `analyzer_compatibility_join_using_top_level_identifier` and a constant projection alias.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=1aefdf9c553447757c0daa4a6d48fa875173b7ee&name_0=MasterCI&name_1=BuzzHouse%20%28amd_ubsan%29

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (c0 UInt32) ENGINE = MergeTree ORDER BY c0;
CREATE TABLE t2 (c0 UInt32) ENGINE = MergeTree ORDER BY c0;

INSERT INTO t1 VALUES (1);
INSERT INTO t2 VALUES (1);

-- The key structure: constant alias `c0` in projection, CROSS JOIN as left table of INNER JOIN USING (c0).
-- Previously caused LOGICAL_ERROR because a ColumnNode was created with CROSS_JOIN as its source.
-- This works because `t1` has a `c0` column, so normal USING resolution finds it.
SELECT (1::UInt32 AS c0)
FROM numbers(1) AS n
CROSS JOIN t1
INNER JOIN t2 USING (c0);

-- This fails because `system.one` has no `c0` column, so the USING key cannot be resolved.
-- Previously caused LOGICAL_ERROR; now correctly returns UNKNOWN_IDENTIFIER.
SELECT (42::UInt32 AS c0)
FROM system.one AS a
CROSS JOIN system.one AS b
INNER JOIN t2 USING (c0); -- { serverError UNKNOWN_IDENTIFIER }

DROP TABLE t1;
DROP TABLE t2;
