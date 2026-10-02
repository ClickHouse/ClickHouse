-- The Azure table functions and engines pick their signature by the number of positional arguments,
-- after `extra_credentials(...)` is taken out and without counting `key = value` arguments. Every
-- statement is logged before it is validated, so the credential has to be hidden at the slot the
-- parser reads it from. Nothing here connects anywhere: EXPLAIN AST only parses.

-- Table functions: the two-argument (url, sas_token) form, also through the cluster and data-lake names.
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f3/cont/data.csv', 'sp=r&sig=SEKRIT_F3'));
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorageCluster('test_shard_localhost', 'http://localhost:11111/visible_f3c/cont/data.csv', 'sp=r&sig=SEKRIT_F3C'));
SELECT count() FROM (EXPLAIN AST SELECT * FROM icebergAzure('http://localhost:11111/visible_f3i/cont/data.csv', 'sp=r&sig=SEKRIT_F3I'));

-- A url carrying a shared access signature, and a connection string followed by an account key: hidden whole.
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/?sp=r&sig=SEKRIT_F5', 'cont_f5', 'blob_f5'));
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=SEKRIT_F6CS;BlobEndpoint=http://localhost:11111/devstoreaccount1;', 'cont_f6', 'blob_f6', 'devstoreaccount1', 'SEKRIT_F6'));

-- `extra_credentials(...)` does not take a slot, wherever it is written.
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f2/cont/data.csv', 'sp=r&sig=SEKRIT_F2', extra_credentials(client_id = 'visible_f2_cid', tenant_id = 'visible_f2_tid')));
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_g4', 'visible_g4_cont', 'visible_g4_blob', extra_credentials(client_id = 'visible_g4_cid', tenant_id = 'visible_g4_tid'), 'visible_g4_acct', 'SEKRIT_G4'));
EXPLAIN AST CREATE TABLE t_e2 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_e2/cont/data.csv', 'sp=r&sig=SEKRIT_E2', extra_credentials(client_id = 'visible_e2_cid', tenant_id = 'visible_e2_tid'));
EXPLAIN AST CREATE TABLE t_g2 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_g2', extra_credentials(client_id = 'visible_g2_cid', tenant_id = 'visible_g2_tid'), 'visible_g2_cont', 'visible_g2_blob', 'visible_g2_acct', 'SEKRIT_G2');

-- Neither does a `key = value` argument, and the value of a key the explicit form does not read is hidden.
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f1/cont/data.csv', 'sp=r&sig=SEKRIT_F1', partition_strategy = 'none'));
EXPLAIN AST CREATE TABLE t_e1 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_e1/cont/data.csv', 'sp=r&sig=SEKRIT_E1', partition_strategy = 'none');
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_f7', 'visible_f7_cont', 'visible_f7_blob', account_key = 'SEKRIT_F7'));

-- Controls, masked the same way before: the engine's two-argument form, an account key at slot 4,
-- a partition strategy override that stays visible, and a connection string that hides only its AccountKey.
EXPLAIN AST CREATE TABLE t_c1 (x UInt8) ENGINE = AzureBlobStorage('http://localhost:11111/visible_c1/cont/data.csv', 'sp=r&sig=SEKRIT_C1');
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_c2', 'visible_c2_cont', 'visible_c2_blob', 'visible_c2_acct', 'SEKRIT_C2'));
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('http://localhost:11111/visible_c3', 'visible_c3_cont', 'visible_c3_blob', 'CSV', 'none', partition_strategy = 'hive'));
SELECT count() FROM (EXPLAIN AST SELECT * FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=visible_c4;AccountKey=SEKRIT_C4;', 'visible_c4_cont', 'visible_c4_blob', 'CSV'));

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
