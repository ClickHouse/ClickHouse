-- Inserting through a TO-target materialized view whose target column is a wider Enum than
-- the view's SELECT output must apply the (valid) Enum widening, like a direct INSERT SELECT,
-- instead of raising a LOGICAL_ERROR "Block structure mismatch ... ConvertingTransform and
-- RemovingReplicatedColumnsTransform" while building the insert pipeline.

DROP TABLE IF EXISTS src8;
DROP TABLE IF EXISTS dst8;
DROP TABLE IF EXISTS mv8;

CREATE TABLE src8 (c1 Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dst8 (c1 Enum8('a' = 1, 'b' = 2, 'c' = 3)) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv8 TO dst8 AS SELECT c1 FROM src8;
INSERT INTO mv8 VALUES ('a'), ('b');
SELECT 'enum8 mergetree', c1, toTypeName(c1) FROM dst8 ORDER BY c1;

DROP TABLE IF EXISTS src16;
DROP TABLE IF EXISTS dst16;
DROP TABLE IF EXISTS mv16;

CREATE TABLE src16 (c1 Enum16('x' = 10, 'y' = 20)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dst16 (c1 Enum16('x' = 10, 'y' = 20, 'z' = 30)) ENGINE = ReplacingMergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv16 TO dst16 AS SELECT c1 FROM src16;
INSERT INTO mv16 VALUES ('y');
INSERT INTO mv16 VALUES ('x');
SELECT 'enum16 replacingmergetree', c1, toTypeName(c1) FROM dst16 ORDER BY c1;

-- A target column that is absent from the view output and has a DEFAULT must still be filled.
DROP TABLE IF EXISTS src_def;
DROP TABLE IF EXISTS dst_def;
DROP TABLE IF EXISTS mv_def;

CREATE TABLE src_def (c1 Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dst_def (c1 Enum8('a' = 1, 'b' = 2, 'c' = 3), extra UInt32 DEFAULT 42) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv_def TO dst_def AS SELECT c1 FROM src_def;
INSERT INTO mv_def VALUES ('a');
SELECT 'enum widening with defaulted column', c1, extra FROM dst_def ORDER BY c1;

DROP TABLE mv8;
DROP TABLE dst8;
DROP TABLE src8;
DROP TABLE mv16;
DROP TABLE dst16;
DROP TABLE src16;
DROP TABLE mv_def;
DROP TABLE dst_def;
DROP TABLE src_def;
