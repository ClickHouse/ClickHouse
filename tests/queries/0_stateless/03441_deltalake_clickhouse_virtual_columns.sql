-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: Depends on AWS
-- Tag no-msan: delta-kernel is not built with msan

SELECT _data_lake_snapshot_version
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', NOSIGN, SETTINGS parallel_replicas_for_cluster_engines = 0)
LIMIT 1;