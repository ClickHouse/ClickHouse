-- Tags: memory-engine
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

-- { echo ON }
CREATE TABLE src (x UInt8) ENGINE = Memory;
CREATE TABLE dst (x UInt8) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv1 TO dst AS SELECT * FROM src;

INSERT INTO src VALUES (0);
SELECT * from dst;

TRUNCATE TABLE dst;

--DROP TABLE src SYNC;
--CREATE TABLE src (y String) ENGINE = MergeTree order by tuple();
ALTER TABLE src ADD COLUMN y UInt8;
ALTER TABLE src DROP COLUMN x;

INSERT INTO src VALUES (0);
SELECT * from dst;
