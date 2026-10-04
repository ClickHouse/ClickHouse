-- Tags: no-fasttest
-- Tag no-fasttest: needs the AzureBlobStorage engine and Azurite, which the fast test does not have

-- AzureBlobStorage engine arguments may end with key-value `partition_strategy` and
-- `partition_columns_in_data_file` arguments. The server itself writes `partition_strategy = 'none'`
-- into the table definition when `PARTITION BY` resolves to no strategy, so such a table must attach again.
-- The write arms use Azurite; the other statements use a SAS token in the URL and stay off the network.

DROP TABLE IF EXISTS t_implicit;
DROP TABLE IF EXISTS t_kv_hive;
DROP TABLE IF EXISTS t_kv_hive_write;
DROP TABLE IF EXISTS t_kv_hive_write_pcdf;
DROP TABLE IF EXISTS t_kv_none;
DROP TABLE IF EXISTS t_positional_equals;

SET file_like_engine_default_partition_strategy = 'wildcard';

CREATE TABLE t_implicit (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV')
PARTITION BY id;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_implicit';
DETACH TABLE t_implicit;
ATTACH TABLE t_implicit;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_implicit';

CREATE TABLE t_kv_hive (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data', 'CSV', partition_strategy = 'hive')
PARTITION BY id;
SELECT name, partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_hive';
DETACH TABLE t_kv_hive;
ATTACH TABLE t_kv_hive;
SELECT name, partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_hive';

-- `num_columns` of the written file: a key-value `hive` keeps the partition column out of it by default.
CREATE TABLE t_kv_hive_write (id UInt64, v String)
ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05291', concat(currentDatabase(), '_kv_hive'), 'Parquet', partition_strategy = 'hive')
PARTITION BY id;
INSERT INTO t_kv_hive_write VALUES (1, 'a');
SELECT DISTINCT num_columns FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05291', concat(currentDatabase(), '_kv_hive/**.parquet'), 'ParquetMetadata') SETTINGS use_hive_partitioning = 0;

CREATE TABLE t_kv_hive_write_pcdf (id UInt64, v String)
ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05291', concat(currentDatabase(), '_kv_hive_pcdf'), 'Parquet', partition_strategy = 'hive', partition_columns_in_data_file = 1)
PARTITION BY id;
INSERT INTO t_kv_hive_write_pcdf VALUES (1, 'a');
SELECT DISTINCT num_columns FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;', 'cont05291', concat(currentDatabase(), '_kv_hive_pcdf/**.parquet'), 'ParquetMetadata') SETTINGS use_hive_partitioning = 0;

SET file_like_engine_default_partition_strategy = 'hive';

CREATE TABLE t_kv_none (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', partition_strategy = 'none')
PARTITION BY id;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_none';
DETACH TABLE t_kv_none;
ATTACH TABLE t_kv_none;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_none';

DESCRIBE TABLE azureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', 'auto', 'id UInt64', partition_strategy = 'none');

-- Cluster queries pass `extra_credentials` on to the replicas, in both argument orders and when parallel replicas
-- turn `azureBlobStorage` into a cluster query. The replicas then refuse to send its token over plain http.
SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', 'auto', 'id UInt64', extra_credentials(client_id = 'x', tenant_id = 'y'), partition_strategy = 'none'); -- { serverError STD_EXCEPTION }
SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', 'auto', 'id UInt64', partition_strategy = 'none', extra_credentials(client_id = 'x', tenant_id = 'y')); -- { serverError STD_EXCEPTION }
SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', 'auto', 'id UInt64', extra_credentials(client_id = 'x', tenant_id = 'y'), partition_strategy = 'none')
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3, parallel_replicas_for_cluster_engines = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'; -- { serverError STD_EXCEPTION }
SYSTEM FLUSH LOGS query_log;
SELECT uniqExact(initial_query_id) FROM system.query_log
WHERE event_date >= yesterday() AND NOT is_initial_query AND query LIKE '%extra_credentials(client_id%'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND query LIKE '%extra_credentials(client_id%');

-- Invalid key-value arguments.
CREATE TABLE t_bad (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', format = 'CSV'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', partition_strategy = 'unknown')
PARTITION BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data', 'CSV', 'auto', 'hive', partition_strategy = 'hive')
PARTITION BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', partition_strategy = 'none', partition_columns_in_data_file = 0)
PARTITION BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bad (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1/cont/data.csv?sig=X', 'X', partition_strategy = 'none'); -- { serverError BAD_ARGUMENTS }

-- A constant `1 = 1` stays a positional argument (`partition_columns_in_data_file`).
CREATE TABLE t_positional_equals (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data', 'CSV', 'auto', 'hive', 1 = 1)
PARTITION BY id;
SELECT name, partition_key FROM system.tables WHERE database = currentDatabase() AND name = 't_positional_equals';

DROP TABLE t_implicit;
DROP TABLE t_kv_hive;
DROP TABLE t_kv_hive_write;
DROP TABLE t_kv_hive_write_pcdf;
DROP TABLE t_kv_none;
DROP TABLE t_positional_equals;
