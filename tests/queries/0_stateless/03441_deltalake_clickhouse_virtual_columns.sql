-- Tags: no-fasttest, no-msan
-- Tag no-fasttest: Depends on AWS
-- Tag no-msan: delta-kernel is not built with msan

SET parallel_replicas_for_cluster_engines = 0;
-- Suppress retryable AWS 5xx noise that would otherwise fail the test via stderr-fatal.
SET send_logs_level = 'fatal';

SELECT _data_lake_snapshot_version
FROM deltaLake('https://clickhouse-public-datasets.s3.amazonaws.com/delta_lake/hits/', NOSIGN)
LIMIT 1;