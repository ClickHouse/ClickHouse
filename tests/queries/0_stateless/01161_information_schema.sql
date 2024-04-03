-- Uppercase/lowercase are okay, mixed case isn't
SHOW TABLES FROM information_schema;
SHOW TABLES FROM INFORMATION_SCHEMA;
SHOW TABLES FROM INFORMATION_schema; -- { serverError UNKNOWN_DATABASE }

DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS mv;
DROP TABLE IF EXISTS tmp;
DROP TABLE IF EXISTS kcu1;
DROP TABLE IF EXISTS kcu2;

CREATE TABLE t (n UInt64, f Float32, s String, fs FixedString(42), d Decimal(9, 6)) ENGINE = Memory;
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE = Null AS SELECT * FROM system.one;
CREATE TEMPORARY TABLE tmp (d Date, dt DateTime, dtms DateTime64(3));
CREATE TABLE kcu1 (i UInt32, s String) ENGINE MergeTree ORDER BY i;
CREATE TABLE kcu2 (i UInt32, d Date, u UUID) ENGINE MergeTree ORDER BY (u, d);


SELECT '-- information_schema.schemata';
SELECT * FROM information_schema.schemata WHERE schema_name ilike 'information_schema' ORDER BY schema_name;

SELECT '-- information_schema.tables';
SELECT * FROM information_schema.tables WHERE table_schema = currentDatabase() AND table_name NOT LIKE '%inner%' ORDER BY table_name;
-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=currentDatabase() ORDER BY schema_name;
-- FIXME #28687

SELECT '-- information_schema.views';
SELECT * FROM information_schema.views WHERE table_schema = currentDatabase() ORDER BY table_name;
-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%'

SELECT '-- information_schema.columns';
SELECT * FROM information_schema.columns WHERE table_schema = currentDatabase() AND table_name NOT LIKE '%inner%' ORDER BY table_name, column_name;

SELECT '-- information_schema.key_column_usage';
SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase() AND table_name = 'kcu1' ORDER BY table_schema, column_name;
SELECT * FROM information_schema.key_column_usage WHERE table_schema = currentDatabase() AND table_name = 'kcu2' ORDER BY table_schema, column_name;

SELECT '-- information_schema.referential_constraints';
SELECT * FROM information_schema.referential_constraints;

SELECT '-- information_schema.statistics';
SELECT * FROM information_schema.statistics;
--
-- mixed upper/lowercase schema and table name:
SELECT count() FROM information_schema.TABLES WHERE table_schema = currentDatabase() AND table_name = 't';
SELECT count() FROM INFORMATION_SCHEMA.tables WHERE table_schema = currentDatabase() AND table_name = 't';
SELECT count() FROM information_schema.taBLES WHERE table_schema  =currentDatabase() AND table_name = 't'; -- { serverError UNKNOWN_TABLE }

DROP VIEW mv;
DROP VIEW v;
DROP TABLE t;
DROP TABLE kcu1;
DROP TABLE kcu2;
