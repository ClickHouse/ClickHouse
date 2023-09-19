SHOW TABLES FROM information_schema;
SHOW TABLES FROM INFORMATION_SCHEMA;

DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS mv;
DROP TABLE IF EXISTS tmp;
DROP TABLE IF EXISTS kcu;
DROP TABLE IF EXISTS kcu2;

CREATE TABLE t (n UInt64, f Float32, s String, fs FixedString(42), d Decimal(9, 6)) ENGINE=Memory;
CREATE VIEW v (n Nullable(Int32), f Float64) AS SELECT n, f FROM t;
CREATE MATERIALIZED VIEW mv ENGINE=Null AS SELECT * FROM system.one;
CREATE TEMPORARY TABLE tmp (d Date, dt DateTime, dtms DateTime64(3));
CREATE TABLE kcu (i UInt32, s String) ENGINE MergeTree ORDER BY i;
CREATE TABLE kcu2 (i UInt32, d Date, u UUID) ENGINE MergeTree ORDER BY (u, d);

-- FIXME #28687
SELECT catalog_name,
       schema_name,
       schema_owner,
       default_character_set_catalog,
       default_character_set_schema,
       default_character_set_name,
       sql_path
FROM information_schema.schemata
WHERE schema_name ilike 'information_schema';

-- SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';

SELECT table_catalog, 
       table_schema, 
       table_name, 
       table_type, 
       table_collation, 
       table_comment
FROM INFORMATION_SCHEMA.TABLES
WHERE (table_schema = currentDatabase() OR table_schema = '')
  AND table_name NOT LIKE '%inner%';

SELECT table_catalog,
       table_schema,
       table_name,
       view_definition,
       check_option,
       is_updatable,
       is_insertable_into,
       is_trigger_updatable,
       is_trigger_deletable,
       is_trigger_insertable_into
FROM information_schema.views
WHERE table_schema = currentDatabase();

-- SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE (TABLE_SCHEMA=currentDatabase() OR TABLE_SCHEMA='') AND TABLE_NAME NOT LIKE '%inner%';

SELECT table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       column_default,
       is_nullable,
       data_type,
       character_maximum_length,
       character_octet_length,
       numeric_precision,
       numeric_precision_radix,
       numeric_scale,
       datetime_precision,
       character_set_catalog,
       character_set_schema,
       character_set_name,
       collation_catalog,
       collation_schema,
       collation_name,
       domain_catalog,
       domain_schema,
       domain_name,
       column_comment,
       column_type
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (table_schema = currentDatabase() OR table_schema = '')
  AND table_name NOT LIKE '%inner%';

-- mixed upper/lowercase schema and table name:
SELECT count() FROM information_schema.TABLES WHERE table_schema=currentDatabase() AND table_name = 't';
SELECT count() FROM INFORMATION_SCHEMA.tables WHERE table_schema=currentDatabase() AND table_name = 't';
SELECT count() FROM INFORMATION_schema.tables WHERE table_schema=currentDatabase() AND table_name = 't'; -- { serverError UNKNOWN_DATABASE }
SELECT count() FROM information_schema.taBLES WHERE table_schema=currentDatabase() AND table_name = 't'; -- { serverError UNKNOWN_TABLE }

SELECT constraint_catalog,
       constraint_schema,
       constraint_name,
       table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       position_in_unique_constraint,
       referenced_table_schema,
       referenced_table_name,
       referenced_column_name
FROM information_schema.key_column_usage
WHERE table_name = 'kcu';

SELECT constraint_catalog,
       constraint_schema,
       constraint_name,
       table_catalog,
       table_schema,
       table_name,
       column_name,
       ordinal_position,
       position_in_unique_constraint,
       referenced_table_schema,
       referenced_table_name,
       referenced_column_name
FROM information_schema.key_column_usage
WHERE table_name = 'kcu2';

SELECT constraint_catalog,
       constraint_name,
       constraint_schema,
       unique_constraint_catalog,
       unique_constraint_name,
       unique_constraint_schema,
       match_option,
       update_rule,
       delete_rule,
       table_name,
       referenced_table_name
FROM information_schema.referential_constraints;

drop view mv;
drop view v;
drop table t;
drop table kcu;
drop table kcu2;
