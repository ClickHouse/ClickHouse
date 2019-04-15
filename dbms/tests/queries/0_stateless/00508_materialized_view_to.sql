DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.mv;

CREATE TABLE test.src (x UInt8) ENGINE = Null;
CREATE TABLE test.dst (x UInt8) ENGINE = Memory;

USE test;

CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM mv ORDER BY x;

-- Detach MV and see if the data is still readable
DETACH TABLE mv;
SELECT * FROM dst ORDER BY x;

USE default;

-- Reattach MV (shortcut)
ATTACH TABLE test.mv;

INSERT INTO test.src VALUES (3);

SELECT * FROM test.mv ORDER BY x;

-- Drop the MV and see if the data is still readable
DROP TABLE test.mv;
SELECT * FROM test.dst ORDER BY x;

DROP TABLE test.src;
DROP TABLE test.dst;
