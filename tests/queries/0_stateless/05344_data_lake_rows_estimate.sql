-- Tags: no-fasttest
-- no-fasttest: the Iceberg table needs the Parquet and Avro libraries.

-- A data lake table keeps its row count in its metadata. The relation estimator reports it, so a
-- join with such a table is estimated like one with any other table, and `EXPLAIN estimates` shows
-- the rows of the read. Without this the read had no estimate and the join none either.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET use_statistics = 0;
SET allow_insert_into_iceberg = 1;
SET use_iceberg_metadata_files_cache = 0;
SET iceberg_delete_data_on_drop = 1;
SET async_insert = 0;

-- Resolve relative user-files paths against the server's data directory.
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05344_data_lake_rows_estimate/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

DROP TABLE IF EXISTS t_dl_dim;
DROP TABLE IF EXISTS t_dl_fact;
CREATE TABLE t_dl_dim (k UInt64, v UInt64)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS iceberg_format_version = 2;
INSERT INTO t_dl_dim SELECT number, number FROM numbers(5);
CREATE TABLE t_dl_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_dl_fact SELECT number % 5, number FROM numbers(100000);

SELECT '-- the lake table reports its 5 rows, and the join is estimated from them';
EXPLAIN estimates = 1 SELECT count() FROM t_dl_fact AS f JOIN t_dl_dim AS d ON f.k = d.k;

SELECT count() FROM t_dl_fact AS f JOIN t_dl_dim AS d ON f.k = d.k;

DROP TABLE t_dl_dim SYNC;
DROP TABLE t_dl_fact;
