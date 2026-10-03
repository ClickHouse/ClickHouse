-- Tags: no-fasttest
-- Definition-supplied Parquet field_id settings are validated at table definition time:
-- engines that freeze the format settings from the CREATE query would otherwise accept an
-- invalid map and then fail every later INSERT, leaving a table that can never be written.

DROP TABLE IF EXISTS t_parquet_field_ids_def;

-- Unknown column.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'missing': '1', 'x': '2', 'y': '3'}; -- { serverError BAD_ARGUMENTS }
-- Value is not an integer.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': 'oops', 'y': '2'}; -- { serverError BAD_ARGUMENTS }
-- Negative id.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '-1', 'y': '2'}; -- { serverError BAD_ARGUMENTS }
-- Out of Int32 range.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '3000000000', 'y': '2'}; -- { serverError BAD_ARGUMENTS }
-- Range reserved by Iceberg for metadata fields.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '2147483540', 'y': '2'}; -- { serverError BAD_ARGUMENTS }
-- Duplicate id.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '1', 'y': '1'}; -- { serverError BAD_ARGUMENTS }
-- The map must cover every column when auto-assign is off.
CREATE TABLE t_parquet_field_ids_def (x Int64, y String) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '1'}; -- { serverError BAD_ARGUMENTS }
-- A nested column must be covered including its subfield paths, not just its top-level name.
CREATE TABLE t_parquet_field_ids_def (t Tuple(a Int64, b Int64)) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'t': '1'}; -- { serverError BAD_ARGUMENTS }
-- Ambiguous flattened paths are rejected with auto-assign, too.
CREATE TABLE t_parquet_field_ids_def (t Tuple(a Int64), `t.a` Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_auto_assign_field_ids = 1; -- { serverError BAD_ARGUMENTS }

-- The same guard covers the URL and object-storage engines.
CREATE TABLE t_parquet_field_ids_def (x Int64) ENGINE = URL('http://localhost:1/none.parquet', Parquet) SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_parquet_field_ids_def (x Int64) ENGINE = S3('http://localhost:11111/test/{database}/none.parquet', NOSIGN, Parquet) SETTINGS output_format_parquet_column_field_ids = {'missing': '1'}; -- { serverError BAD_ARGUMENTS }
-- Without a declared column list the header-independent checks still run.
CREATE TABLE t_parquet_field_ids_def ENGINE = S3('http://localhost:11111/test/{database}/none.parquet', NOSIGN, Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': 'oops'}; -- { serverError BAD_ARGUMENTS }

-- Schema-inference cases that need a pre-existing data file live in
-- 04823_parquet_field_ids_schema_inference.sh, which builds a per-run-unique file path
-- (a fixed path here would race with concurrent runs of the same test).

-- A valid definition works, and replaying it (DETACH/ATTACH) is not re-validated.
CREATE TABLE t_parquet_field_ids_def (x Int64, t Tuple(a Int64, b Int64)) ENGINE = File(Parquet) SETTINGS output_format_parquet_column_field_ids = {'x': '1', 't': '2', 't.a': '3', 't.b': '4'};
INSERT INTO t_parquet_field_ids_def VALUES (42, (1, 2));
SELECT * FROM t_parquet_field_ids_def;
DETACH TABLE t_parquet_field_ids_def;
ATTACH TABLE t_parquet_field_ids_def;
SELECT * FROM t_parquet_field_ids_def;
DROP TABLE t_parquet_field_ids_def;

CREATE TABLE t_parquet_field_ids_def (x Int64) ENGINE = File(Parquet) SETTINGS output_format_parquet_auto_assign_field_ids = 1;
INSERT INTO t_parquet_field_ids_def VALUES (7);
SELECT * FROM t_parquet_field_ids_def;
DROP TABLE t_parquet_field_ids_def;
