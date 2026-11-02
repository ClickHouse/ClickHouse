-- Regression test: Bad cast from ConstantNode to ListNode when JOIN USING
-- resolves identifier from a projection alias (with analyzer_compatibility_join_using_top_level_identifier).
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=2d046da0e9520c48cd6d1c01eda29f76dcc4f93c&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_tsan%29

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id String, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id String, code String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id String, code String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES ('a', '1');
INSERT INTO t2 VALUES ('a', 'x');
INSERT INTO t3 VALUES ('a', 'y');

SET enable_analyzer = 1;
SET analyzer_compatibility_join_using_top_level_identifier = 1;

-- Previously caused LOGICAL_ERROR: Bad cast from type DB::ConstantNode to DB::ListNode.
SELECT t1.val, concat('_1', 2, 2) AS id
FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 USING (id)
ORDER BY t1.val ASC; -- { serverError AMBIGUOUS_IDENTIFIER }

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
