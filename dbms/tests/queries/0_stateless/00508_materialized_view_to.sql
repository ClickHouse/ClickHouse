DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x UInt8) ENGINE = Null;
CREATE TABLE dst (x UInt8) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (1), (2);
SELECT * FROM mv ORDER BY x;

-- Detach MV and see if the data is still readable
DETACH TABLE mv;
SELECT * FROM dst ORDER BY x;

USE default;

-- Reattach MV (shortcut)
ATTACH TABLE mv;

INSERT INTO src VALUES (3);

SELECT * FROM mv ORDER BY x;

-- Drop the MV and see if the data is still readable
DROP TABLE mv;
SELECT * FROM dst ORDER BY x;

DROP TABLE src;
DROP TABLE dst;
