-- Tags: no-fasttest
-- no-fasttest: the `DataLakeCatalog` database engine needs the Avro and Parquet libraries.

-- `SHOW CREATE DATABASE` of a `DataLakeCatalog` database shows its `COMMENT`, and `ALTER DATABASE ... MODIFY SETTING` keeps it in the stored metadata.
-- `ATTACH` does not connect to the catalog, so the endpoint does not have to exist.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', warehouse = 'demo', auth_header = 'Authorization: Bearer a'
COMMENT 'dl comment';

SHOW CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

ALTER DATABASE {CLICKHOUSE_DATABASE_1:Identifier} MODIFY SETTING auth_header = 'Authorization: Bearer b';
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT comment FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
