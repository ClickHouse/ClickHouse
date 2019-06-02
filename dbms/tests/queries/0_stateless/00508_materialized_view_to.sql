CREATE DATABASE test;
USE test;

CREATE TABLE src (x UInt8) ENGINE = Null;
CREATE TABLE dst (x UInt8) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv_00508 TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM mv_00508 ORDER BY x;

-- Detach MV and see if the data is still readable
DETACH TABLE mv_00508;
SELECT * FROM dst ORDER BY x;

USE default;

-- Reattach MV (shortcut)
ATTACH TABLE test.mv_00508;

INSERT INTO test.src VALUES (3);

SELECT * FROM test.mv_00508 ORDER BY x;

-- Drop the MV and see if the data is still readable
DROP TABLE test.mv_00508;
SELECT * FROM test.dst ORDER BY x;

DROP TABLE test.src;
DROP TABLE test.dst;
