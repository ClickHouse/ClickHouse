-- Tags: no-fasttest
-- Tag no-fasttest: the AzureBlobStorage engine is not available in the fast test build

-- AzureBlobStorage engine arguments may end with key-value `partition_strategy` and
-- `partition_columns_in_data_file` arguments. The server itself writes `partition_strategy = 'none'`
-- into the table definition when `PARTITION BY` resolves to no strategy, so such a table must attach again.
-- The SAS token in the URL keeps every statement off the network.

DROP TABLE IF EXISTS t_implicit;
DROP TABLE IF EXISTS t_kv_hive;
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

SET file_like_engine_default_partition_strategy = 'hive';

CREATE TABLE t_kv_none (id UInt64, v String)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', partition_strategy = 'none')
PARTITION BY id;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_none';
DETACH TABLE t_kv_none;
ATTACH TABLE t_kv_none;
SELECT name, empty(partition_key) FROM system.tables WHERE database = currentDatabase() AND name = 't_kv_none';

DESCRIBE TABLE azureBlobStorage('http://localhost:11111/devstoreaccount1?sig=X', 'cont', 'data.csv', 'CSV', 'auto', 'id UInt64', partition_strategy = 'none');

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
DROP TABLE t_kv_none;
DROP TABLE t_positional_equals;
