-- Tags: no-fasttest
-- no-fasttest: the AzureBlobStorage engine and the azureBlobStorage table function are not in the fast test build.

-- An Azure storage account url can carry a shared access signature in its query string, and that is
-- what the engine authenticates with when the url contains a `?`. The `sig` parameter is the
-- signature itself, the only unguessable part of the token; it used to stay verbatim in SHOW CREATE,
-- in system.tables.engine_full and in the query tree dump, while the same credential spelled as a
-- connection string (`SharedAccessSignature=`) was hidden. The remaining parameters (version,
-- permissions, expiry, resource) grant nothing on their own and stay visible.

SET enable_analyzer = 1;
SET format_display_secrets_in_show_and_select = 0;

DROP TABLE IF EXISTS t_azure_sas;
CREATE TABLE t_azure_sas (x UInt8)
ENGINE = AzureBlobStorage('http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sp=rl&sr=c&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');
SHOW CREATE TABLE t_azure_sas;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 't_azure_sas';
DROP TABLE t_azure_sas;

-- The table function and its cluster variant take the same url argument; run_passes = 0 leaves them
-- unresolved, so nothing is read.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_SIGNATURE'), '', 'data.parquet', 'Parquet');

-- `AzureBlobStorage::isConnectionString` is a case sensitive `starts_with("http")`, so a mixed case
-- scheme is classified as a connection string and the credential never authenticates. The signature
-- the user typed still reaches the log, so it is masked in both spellings.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('HTTPS://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sig=SEKRIT_UPPERCASE_SCHEME', '', 'data.parquet', 'Parquet');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorageCluster('c', 'http://localhost:11111/devstoreaccount1/cont?sv=2025-01-05&sig=SEKRIT_SIGNATURE', '', 'data.parquet', 'Parquet');

-- A computed named override key can evaluate to storage_account_url. Hide its value whole when the
-- key cannot be reconstructed from the AST, before the named collection is resolved.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, concat('storage_account_', 'url') = concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_SIGNATURE'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, concat('http://localhost:11111/devstoreaccount1/cont?sig=', 'SEKRIT_NAMED_EXPRESSION'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, client_id = 'SEKRIT_CLIENT_ID', tenant_id = 'SEKRIT_TENANT_ID');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage(nc_04909_missing, storage_account_url = 'HTTPS://localhost:11111/devstoreaccount1/cont?sig=SEKRIT_UPPERCASE_OVERRIDE');

-- extra_credentials is removed before positional arguments are assigned. The account key after it
-- must still occupy the account_key slot.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1', 'cont', 'data.parquet', extra_credentials(client_id = 'id', tenant_id = 'tenant'), 'account', 'SEKRIT_POSITIONAL_KEY');

-- headers is also removed before positional arguments are assigned.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1', 'cont', 'data.parquet', headers('Authorization' = 'SEKRIT_HEADER'), 'account', 'SEKRIT_KEY_AFTER_HEADERS');

-- A named argument in a direct call is invalid, but the query is formatted before validation. Hide
-- that value and every following positional argument because their intended slots are ambiguous.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1', container = 'SEKRIT_CONTAINER', 'data.parquet', 'account', 'SEKRIT_AFTER_NAMED');

-- A url with no signature in it is left alone.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM azureBlobStorage('http://localhost:11111/devstoreaccount1/cont', 'cont', 'data.parquet', 'Parquet');

-- The unified url entrypoints dispatch Azure schemes to AzureBlobStorage and must mask the same SAS.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url('az://account.blob.core.windows.net/cont/data.csv?sp=r&sig=SEKRIT_URL_FUNCTION', 'CSV');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04909_missing, url = 'azure://account.blob.core.windows.net/cont/data.csv?sp=r&sig=SEKRIT_URL_OVERRIDE');

-- After a named collection the parser rejects a positional argument and ignores an unexpected
-- function, both after the query is formatted, so both must be hidden whole.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04909_missing, 'az://account.blob.core.windows.net/cont/data.csv?sp=r&sig=SEKRIT_URL_POSITIONAL');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM url(nc_04909_missing, concat('az://account.blob.core.windows.net/cont/data.csv?sp=r&sig=', 'SEKRIT_URL_EXPRESSION'));

DROP TABLE IF EXISTS t_url_azure_sas;
CREATE TABLE t_url_azure_sas (x UInt8)
ENGINE = URL('abfss://cont@account.dfs.core.windows.net/data.csv?sp=r&sig=SEKRIT_URL_ENGINE', 'CSV');
SHOW CREATE TABLE t_url_azure_sas;
DROP TABLE t_url_azure_sas;

-- The URL database engine reaches the same Azure backends through its base url.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = URL('az://account.blob.core.windows.net/cont?sp=r&sig=SEKRIT_URL_DATABASE');
SELECT engine_full FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
