-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: needs Parquet, s3 and IcebergLocal
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`
-- Random settings limits: optimize_move_to_prewhere=(1, 1); query_plan_remove_unused_columns=(1, 1)

-- A Parquet read with PREWHERE must still return the subcolumns that the format reads through their
-- whole column (`n.null`, `arr.size0`), also when nothing above uses them (`indexHint`).

INSERT INTO FUNCTION file(currentDatabase() || '_05339.parquet')
SELECT number, if(number % 3 = 0, NULL, number) AS n, range(number % 4) AS arr
FROM numbers(100)
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'file';
SELECT count(), sum(number) FROM file(currentDatabase() || '_05339.parquet') WHERE indexHint(n.null) AND number > 5;
SELECT countIf(n.null), sum(arr.size0), count() FROM file(currentDatabase() || '_05339.parquet') WHERE number > 5;
SELECT countIf(n.null), count() FROM file(currentDatabase() || '_05339.parquet') PREWHERE n > 5 OR number < 3;
SELECT sum(arr.size0), count() FROM file(currentDatabase() || '_05339.parquet') PREWHERE length(arr) > 1;
SELECT n.null, number FROM file(currentDatabase() || '_05339.parquet') WHERE number > 5 ORDER BY number LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SELECT arr.size0, number FROM file(currentDatabase() || '_05339.parquet') WHERE number > 5 ORDER BY number LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05339.parquet', format = Parquet)
SELECT number, if(number % 3 = 0, NULL, number) AS n, range(number % 4) AS arr
FROM numbers(100)
SETTINGS s3_truncate_on_insert = 1;

SELECT 's3';
-- With parallel replicas `s3` is read through its `-Cluster` storage, which rejects PREWHERE.
SELECT countIf(n.null), count() FROM s3(s3_conn, filename = currentDatabase() || '/05339.parquet', format = Parquet) PREWHERE n > 5 OR number < 3
SETTINGS parallel_replicas_for_cluster_engines = 0;
SELECT n.null, number FROM s3(s3_conn, filename = currentDatabase() || '/05339.parquet', format = Parquet) WHERE number > 5 ORDER BY number LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

-- An ORC data file of an Iceberg table read as Parquet: PREWHERE is applied after the reader.
SET allow_insert_into_iceberg = 1;
SET async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05339_iceberg_orc/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t_05339_orc (number Int64, n Nullable(Int64)) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'ORC');
INSERT INTO t_05339_orc SELECT number, if(number % 3 = 0, NULL, number) FROM numbers(100);
CREATE TABLE t_05339_parquet ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');

SELECT 'iceberg orc';
SELECT countIf(n.null), count() FROM t_05339_parquet PREWHERE n > 5 OR number < 3;

DROP TABLE t_05339_parquet SYNC;
DROP TABLE t_05339_orc SYNC;
DROP TABLE iceberg_path;
