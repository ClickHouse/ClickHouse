SET compile_expressions = 1;
SET min_count_to_compile = 1;
SET optimize_move_to_prewhere = 0;
SET enable_optimize_predicate_expression=0;

DROP TABLE IF EXISTS test.dt;
DROP TABLE IF EXISTS test.testx;

CREATE TABLE test.dt(tkey Int32) ENGINE = MergeTree order by tuple();
INSERT INTO test.dt VALUES (300000);
CREATE TABLE test.testx(t Int32, a UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test.testx VALUES (100000, 0);

SELECT COUNT(*) FROM test.testx WHERE NOT a AND t < (SELECT tkey FROM test.dt);

DROP TABLE test.dt;
CREATE TABLE test.dt(tkey Int32) ENGINE = MergeTree order by tuple();
INSERT INTO test.dt VALUES (0);

SELECT COUNT(*) FROM test.testx WHERE NOT a AND t < (SELECT tkey FROM test.dt);

DROP TABLE IF EXISTS test.dt;
DROP TABLE IF EXISTS test.testx;
