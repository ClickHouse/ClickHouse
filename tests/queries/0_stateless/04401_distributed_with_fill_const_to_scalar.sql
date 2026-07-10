-- Tags: distributed

-- Test ORDER BY ... WITH FILL FROM/TO/STEP with optimize_const_name_size on distributed tables.
-- ReplaceLongConstWithScalarVisitor replaced WITH FILL constants with __getScalar function nodes,
-- and the planner reads them directly via as<ConstantNode &>() in extractWithFillValue,
-- causing "Bad cast from type DB::FunctionNode to DB::ConstantNode".
-- optimize_const_name_size = 0 replaces ALL constants with __getScalar calls.

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't0');

INSERT INTO t0 VALUES (1), (3), (5);

SELECT c0 FROM t1 ORDER BY c0 WITH FILL TO 6 SETTINGS optimize_const_name_size = 0;
SELECT c0 FROM t1 ORDER BY c0 WITH FILL FROM 0 TO 6 SETTINGS optimize_const_name_size = 0;
SELECT c0 FROM t1 ORDER BY c0 WITH FILL FROM 0 TO 6 STEP 2 SETTINGS optimize_const_name_size = 0;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;
