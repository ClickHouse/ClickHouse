-- Tags: no-replicated-database
-- Test that dropping and recreating a materialized view with the same name
-- does not cause assertion failures due to stale view dependencies.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src;

INSERT INTO src VALUES (1);
SELECT x FROM dst ORDER BY x;

-- Drop the MV and recreate it with a different source table
DROP TABLE mv;

CREATE TABLE src2 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT x FROM src2;

INSERT INTO src2 VALUES (2);
SELECT x FROM dst ORDER BY x;

-- Alter the recreated MV to read from the original source
ALTER TABLE mv MODIFY QUERY SELECT x FROM src;

INSERT INTO src VALUES (3);
SELECT x FROM dst ORDER BY x;

-- Test exchange tables with MV
DROP TABLE IF EXISTS mv2;
CREATE TABLE dst2 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv2 TO dst2 AS SELECT x FROM src2;

INSERT INTO src2 VALUES (4);
SELECT x FROM dst2 ORDER BY x;

-- Exchange MVs
EXCHANGE TABLES mv AND mv2;

INSERT INTO src VALUES (5);
SELECT x FROM dst2 ORDER BY x;

INSERT INTO src2 VALUES (6);
SELECT x FROM dst ORDER BY x;

DROP TABLE mv;
DROP TABLE mv2;
DROP TABLE src;
DROP TABLE src2;
DROP TABLE dst;
DROP TABLE dst2;
