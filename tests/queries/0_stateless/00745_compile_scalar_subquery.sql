SET compile_expressions = 1;
SET min_count_to_compile_expression = 1;
SET optimize_move_to_prewhere = 0;

DROP TABLE IF EXISTS dt;
DROP TABLE IF EXISTS testx;

CREATE TABLE dt(tkey Int32) ENGINE = MergeTree order by tuple();
INSERT INTO dt VALUES (300000);
CREATE TABLE testx(t Int32, a UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO testx VALUES (100000, 0);

SELECT COUNT(*) FROM testx WHERE NOT a AND t < (SELECT tkey FROM dt);

DROP TABLE dt;
CREATE TABLE dt(tkey Int32) ENGINE = MergeTree order by tuple();
INSERT INTO dt VALUES (0);

SELECT COUNT(*) FROM testx WHERE NOT a AND t < (SELECT tkey FROM dt);

DROP TABLE IF EXISTS dt;
DROP TABLE IF EXISTS testx;
