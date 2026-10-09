-- Tags: no-fasttest, no-replicated-database
-- no-fasttest: needs the Parquet format and IcebergLocal.
-- no-replicated-database: the replicated DDL worker evaluates the IcebergLocal path without the session's temporary table.
-- Random settings limits: optimize_move_to_prewhere=(1, 1)

-- A predicate evaluated while reading Parquet (PREWHERE, row policy) and the filters a data lake read applies
-- after reading must use the query's short_circuit_function_evaluation: the guarded intDiv must not run for x = 0.

SET engine_file_truncate_on_insert = 1;
SET async_insert = 0;
-- The INSERT into the File table must not be executed by several replicas writing one file (as in 05227).
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05339.parquet', Parquet, 'x Int32, y String')
SELECT number % 3, toString(number) FROM numbers(30);

SELECT count(), sum(length(y)) FROM file(currentDatabase() || '_05339.parquet', Parquet, 'x Int32, y String') WHERE x = 0 OR intDiv(10, x) > 1;
SELECT count(), sum(length(y)) FROM file(currentDatabase() || '_05339.parquet', Parquet, 'x Int32, y String') WHERE if(x = 0, 0, intDiv(10, x) > 1);
SELECT count() FROM file(currentDatabase() || '_05339.parquet', Parquet, 'x Int32, y String') PREWHERE x = 0 OR intDiv(10, x) > 1;
-- The setting is taken from the query, not forced on.
SELECT count() FROM file(currentDatabase() || '_05339.parquet', Parquet, 'x Int32, y String') PREWHERE x = 0 OR intDiv(10, x) > 1 SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }

DROP TABLE IF EXISTS t_05339_file;
CREATE TABLE t_05339_file (x Int32, y String) ENGINE = File(Parquet);
INSERT INTO t_05339_file SELECT number % 3, toString(number) FROM numbers(30);
CREATE ROW POLICY OR REPLACE p_05339_file ON t_05339_file USING if(x = 0, 0, intDiv(10, x) > 1) TO ALL;
SELECT count(), sum(length(y)) FROM t_05339_file;
DROP ROW POLICY p_05339_file ON t_05339_file;
DROP TABLE t_05339_file;

-- After RENAME COLUMN the data file keeps the old name, so the filters are applied after the read.
SET allow_insert_into_iceberg = 1;
SET allow_experimental_iceberg_compaction = 0;
SET use_iceberg_metadata_files_cache = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05339_parquet_prewhere_short_circuit/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

DROP TABLE IF EXISTS t_05339_iceberg;
CREATE TABLE t_05339_iceberg (x Int32, y String)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
INSERT INTO t_05339_iceberg SELECT number % 3, toString(number) FROM numbers(30);
ALTER TABLE t_05339_iceberg RENAME COLUMN x TO z;

SELECT count(), sum(length(y)) FROM t_05339_iceberg WHERE z = 0 OR intDiv(10, z) > 1;
SELECT count(), sum(length(y)) FROM t_05339_iceberg WHERE z = 0 OR intDiv(10, z) > 1 SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }
CREATE ROW POLICY OR REPLACE p_05339_iceberg ON t_05339_iceberg USING if(z = 0, 0, intDiv(10, z) > 1) TO ALL;
SELECT count(), sum(length(y)) FROM t_05339_iceberg;
SELECT count(), sum(length(y)) FROM t_05339_iceberg SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError ILLEGAL_DIVISION }
DROP ROW POLICY p_05339_iceberg ON t_05339_iceberg;
DROP TABLE t_05339_iceberg;
