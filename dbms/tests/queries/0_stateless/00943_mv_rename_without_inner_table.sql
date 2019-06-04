DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.original_mv;
DROP TABLE IF EXISTS test.new_mv;

CREATE TABLE test.src (x UInt8) ENGINE = Null;
CREATE TABLE test.dst (x UInt8) ENGINE = Memory;

USE test;

CREATE MATERIALIZED VIEW test.original_mv TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM test.original_mv ORDER BY x;

RENAME TABLE test.original_mv TO test.new_mv;

INSERT INTO src VALUES (3);
SELECT * FROM dst ORDER BY x;

SELECT * FROM test.new_mv ORDER BY x;

DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.original_mv;
DROP TABLE IF EXISTS test.new_mv;
