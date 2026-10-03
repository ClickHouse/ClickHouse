-- Tags: no-fasttest
-- no-fasttest: a CREATE with a SETTINGS clause needs the Azure table engines, which the fast test build does not have.

-- The Azure table functions and engines pick their signature by the number of positional arguments,
-- after `extra_credentials(...)` is taken out and without counting `key = value` arguments. Every
-- statement is logged before it is validated, so the credential has to be hidden at the slot the
-- parser reads it from. Nothing here connects anywhere: EXPLAIN AST only parses.

-- Table functions: the two-argument (url, sas_token) form, also through the cluster and data-lake names.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f3/cont/data.csv', 'sp=r&sig=SEKRIT_F3'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'http://localhost:11111/visible_f3c/cont/data.csv', 'sp=r&sig=SEKRIT_F3C'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM icebergAzure('http://localhost:11111/visible_f3i/cont/data.csv', 'sp=r&sig=SEKRIT_F3I'));

-- A url carrying a shared access signature, and a connection string followed by an account key: hidden whole.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/?sp=r&sig=SEKRIT_F5', 'cont_f5', 'blob_f5'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=SEKRIT_F6CS;BlobEndpoint=http://localhost:11111/devstoreaccount1;', 'cont_f6', 'blob_f6', 'devstoreaccount1', 'SEKRIT_F6'));

-- `extra_credentials(...)` does not take a slot, wherever it is written.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f2/cont/data.csv', 'sp=r&sig=SEKRIT_F2', extra_credentials(client_id = 'visible_f2_cid', tenant_id = 'visible_f2_tid')));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_g4', 'visible_g4_cont', 'visible_g4_blob', extra_credentials(client_id = 'visible_g4_cid', tenant_id = 'visible_g4_tid'), 'visible_g4_acct', 'SEKRIT_G4'));
EXPLAIN AST CREATE TABLE t_e2 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_e2/cont/data.csv', 'sp=r&sig=SEKRIT_E2', extra_credentials(client_id = 'visible_e2_cid', tenant_id = 'visible_e2_tid'));
EXPLAIN AST CREATE TABLE t_g2 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_g2', extra_credentials(client_id = 'visible_g2_cid', tenant_id = 'visible_g2_tid'), 'visible_g2_cont', 'visible_g2_blob', 'visible_g2_acct', 'SEKRIT_G2');
-- Only the first one is taken out, so with two the slots cannot be established and every argument is hidden.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/url_m2', 'cont_m2', 'blob_m2', extra_credentials(client_id = 'visible_m2_cid'), extra_credentials(tenant_id = 'visible_m2_tid'), 'SEKRIT_M2P', 'SEKRIT_M2S'));

-- Inside `extra_credentials(...)` only `client_id` and `tenant_id` are shown.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_x1/cont/data.csv', 'sp=r&sig=SEKRIT_X1S', extra_credentials(client_id = 'visible_x1_cid', client_secret = 'SEKRIT_X1')));
EXPLAIN AST CREATE TABLE t_x2 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_x2', 'visible_x2_cont', 'visible_x2_blob', 'CSV', extra_credentials(tenant_id = 'visible_x2_tid', client_secret = 'SEKRIT_X2'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_x3/cont/data.csv', 'sp=r&sig=SEKRIT_X3S', extra_credentials(client_id = 'visible_x3_cid', role_arn = 'SEKRIT_X3')));
-- S3 shows only its own `role_arn`.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM s3('http://localhost:11111/test/visible_x4.csv', extra_credentials(role_arn = 'visible_x4_arn', client_id = 'SEKRIT_X4C', tenant_id = 'SEKRIT_X4T')));

-- A connection string is read part by part and a repeated key keeps its last value, so every
-- `AccountKey` and `SharedAccessSignature` is hidden, and so is an endpoint url with a query.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=visible_cs1;AccountKey=SEKRIT_CS1K;SharedAccessSignature=sp=r&sig=SEKRIT_CS1S;BlobEndpoint=http://localhost:11111/visible_cs1;', 'visible_cs1_cont', 'visible_cs1_blob', 'CSV'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('AccountName=visible_cs2;BlobEndpoint=http://localhost:11111/visible_cs2/?sp=r&sig=SEKRIT_CS2;', 'visible_cs2_cont', 'visible_cs2_blob'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('AccountName=visible_cs3;AccountKey=SEKRIT_CS3A;AccountKey=SEKRIT_CS3B;', 'visible_cs3_cont', 'visible_cs3_blob', 'CSV'));
EXPLAIN AST CREATE TABLE t_cs4 (x UInt8) ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=visible_cs4;AccountKey=SEKRIT_CS4K;SharedAccessSignature=sp=r&sig=SEKRIT_CS4S;BlobEndpoint=http://localhost:11111/visible_cs4;', 'visible_cs4_cont', 'visible_cs4_blob', 'CSV');
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage(nc_05317_missing, connection_string = 'AccountName=visible_cs5;AccountKey=SEKRIT_CS5K;SharedAccessSignature=sp=r&sig=SEKRIT_CS5S', container = 'visible_cs5_cont', blob_path = 'visible_cs5_blob'));
EXPLAIN AST CREATE TABLE t_cs6 (x UInt8) ENGINE = AzureQueue('http://localhost:11111/visible_cs6', 'visible_cs6_cont', '*', 'CSV') SETTINGS mode = 'unordered', after_processing = 'move', after_processing_move_connection_string = 'BlobEndpoint=http://localhost:11111/visible_cs6/?sp=r&sig=SEKRIT_CS6;AccountKey=SEKRIT_CS6A;AccountKey=SEKRIT_CS6B';
-- A url in that setting is read with its shared access signature after '?', so it is hidden whole.
EXPLAIN AST CREATE TABLE t_q1 (x UInt8) ENGINE = AzureQueue('http://localhost:11111/visible_q1', 'visible_q1_cont', '*', 'CSV') SETTINGS mode = 'unordered', after_processing = 'move', after_processing_move_connection_string = 'https://url_q1.blob.core.windows.net/?sp=rw&sig=SEKRIT_Q1', after_processing_move_container = 'visible_q1_mc';

-- The named-collection form hides everything when an override can carry a credential the rules here cannot mask.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage(nc_05317_missing, storage_account_url = 'https://visible_n1.blob.core.windows.net/?sp=r&sig=SEKRIT_N1', container = 'visible_n1_cont', blob_path = 'visible_n1_blob'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage(nc_05317_missing, connection_string = 'AccountName=visible_n2;AccountKey=SEKRIT_N2K', account_key = 'SEKRIT_N2', container = 'visible_n2_cont', blob_path = 'visible_n2_blob'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorageCluster('test_shard_localhost', nc_05317_missing, storage_account_url = 'https://visible_n3.blob.core.windows.net/?sp=r&sig=SEKRIT_N3', container = 'visible_n3_cont', blob_path = 'visible_n3_blob'));

-- In the cluster forms the first argument is the cluster name, so a call there hides everything.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorageCluster(extra_credentials(client_id = 'visible_k1_cid'), 'http://localhost:11111/url_k1/cont/data.csv', 'sp=r&sig=SEKRIT_K1'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorageCluster(partition_strategy = 'none', 'http://localhost:11111/url_k2/cont/data.csv', 'sp=r&sig=SEKRIT_K2'));

-- Neither does a `key = value` argument, and the value of a key the explicit form does not read is hidden.
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f1/cont/data.csv', 'sp=r&sig=SEKRIT_F1', partition_strategy = 'none'));
EXPLAIN AST CREATE TABLE t_e1 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_e1/cont/data.csv', 'sp=r&sig=SEKRIT_E1', partition_strategy = 'none');
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f7', 'visible_f7_cont', 'visible_f7_blob', account_key = 'SEKRIT_F7'));

-- Controls, masked the same way before: the engine's two-argument form, an account key at slot 4,
-- a partition strategy override that stays visible, a connection string that hides only its AccountKey,
-- the account key override of a named collection, and a plain move url.
EXPLAIN AST CREATE TABLE t_c1 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_c1/cont/data.csv', 'sp=r&sig=SEKRIT_C1');
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_c2', 'visible_c2_cont', 'visible_c2_blob', 'visible_c2_acct', 'SEKRIT_C2'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_c3', 'visible_c3_cont', 'visible_c3_blob', 'CSV', 'none', partition_strategy = 'hive'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=visible_c4;AccountKey=SEKRIT_C4;', 'visible_c4_cont', 'visible_c4_blob', 'CSV'));
SELECT count() > 0 FROM (EXPLAIN AST SELECT * FROM azureBlobStorage(nc_05317_missing, storage_account_url = 'http://localhost:11111/visible_c5', container = 'visible_c5_cont', blob_path = 'visible_c5_blob', account_name = 'visible_c5_acct', account_key = 'SEKRIT_C5'));
EXPLAIN AST CREATE TABLE t_c6 (x UInt8) ENGINE = AzureQueue('http://localhost:11111/visible_c6', 'visible_c6_cont', '*', 'CSV') SETTINGS mode = 'unordered', after_processing = 'move', after_processing_move_connection_string = 'http://localhost:11111/visible_c6_move', after_processing_move_container = 'visible_c6_mc';

SYSTEM FLUSH LOGS query_log;

-- The logged text of every statement above, in execution order.
SELECT query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_id = initial_query_id
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
  AND query ILIKE '%EXPLAIN AST%'
  AND query NOT ILIKE '%system.query_log%'
ORDER BY event_time_microseconds;

SELECT count() > 0, countIf(query LIKE '%SEKRIT%')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
  AND query ILIKE '%EXPLAIN AST%'
  AND query NOT ILIKE '%system.query_log%';
