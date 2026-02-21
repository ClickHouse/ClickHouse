-- Tags: no-random-settings

-- Test: schema_inference_make_columns_nullable_if_not_in_keys strips Nullable from inferred key columns.

SET schema_inference_make_columns_nullable = 1;
SET schema_inference_make_columns_nullable_if_not_in_keys = 1;

-- CREATE TABLE ... AS SELECT FROM format(): key column stripped of Nullable, others keep Nullable.
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 ENGINE = MergeTree ORDER BY c1 AS SELECT * FROM format(CSV, '1,hello,2.5');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't1' ORDER BY name;
DROP TABLE t1;

-- Multiple key columns.
DROP TABLE IF EXISTS t2;
CREATE TABLE t2 ENGINE = MergeTree ORDER BY (c1, c2) AS SELECT * FROM format(CSV, '1,hello,2.5');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't2' ORDER BY name;
DROP TABLE t2;

-- Setting off: all columns remain Nullable (with allow_nullable_key).
SET schema_inference_make_columns_nullable_if_not_in_keys = 0;
DROP TABLE IF EXISTS t3;
CREATE TABLE t3 ENGINE = MergeTree ORDER BY c1 SETTINGS allow_nullable_key = 1 AS SELECT * FROM format(CSV, '1,hello,2.5');
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't3' ORDER BY name;
DROP TABLE t3;
