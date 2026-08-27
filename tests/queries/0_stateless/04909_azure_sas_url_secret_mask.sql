-- Tags: no-fasttest
-- no-fasttest: the AzureBlobStorage engine and the azureBlobStorage table function are not in the fast test build.

-- An Azure storage account url can carry a shared access signature in its query string, and that is
-- what the engine authenticates with when the url contains a `?`. The `sig` parameter is the
-- signature itself, the only unguessable part of the token; it used to stay verbatim in SHOW CREATE,
-- in system.tables.engine_full and in the query tree dump, while the same credential spelled as a
-- connection string (`SharedAccessSignature=`) was hidden. The remaining parameters (version,
-- permissions, expiry, resource) grant nothing on their own and stay visible.

SET format_display_secrets_in_show_and_select = 0;

DROP TABLE IF EXISTS t_azure_sas;
CREATE TABLE t_azure_sas (x UInt8)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sp=rl&sr=c&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');
SHOW CREATE TABLE t_azure_sas;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_azure_sas';
DROP TABLE t_azure_sas;

-- The table function and its cluster variant take the same url argument; run_passes = 0 leaves them
-- unresolved, so nothing is read.
SET enable_analyzer = 1;
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_SIGNATURE'), '', 'data.parquet', 'Parquet');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorageCluster('c', 'http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');

-- A computed named override key can evaluate to storage_account_url. Hide its value whole when the
-- key cannot be reconstructed from the AST, before the named collection is resolved.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, concat('storage_account_', 'url') = concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_SIGNATURE'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_NAMED_EXPRESSION'));

-- extra_credentials is removed before positional arguments are assigned. The account key after it
-- must still occupy the account_key slot.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1', 'cont', 'data.parquet', extra_credentials(client_id = 'id', tenant_id = 'tenant'), 'account', 'SEKRIT_POSITIONAL_KEY');

-- A url with no signature in it is left alone.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/cont', 'cont', 'data.parquet', 'Parquet');
