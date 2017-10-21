DROP TABLE IF EXISTS test.src;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.mv;

CREATE TABLE test.src (x UInt8) ENGINE = Null;
CREATE TABLE test.dst (x UInt8) ENGINE = Memory();

CREATE MATERIALIZED VIEW test.mv TO test.dst AS SELECT * FROM test.src;
INSERT INTO test.src VALUES (1), (2);

-- Detach MV and see if the data is still readable
DETACH TABLE test.mv;
SELECT * FROM test.dst;

-- Reattach MV (shortcut)
ATTACH TABLE test.mv;

-- Drop the MV and see if the data is still readable
DROP TABLE test.mv;
SELECT * FROM test.dst;

DROP TABLE test.src;
DROP TABLE test.dst;
