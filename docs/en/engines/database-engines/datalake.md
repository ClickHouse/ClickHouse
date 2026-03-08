---
description: 'The DataLakeCatalog database engine enables you to connect ClickHouse to external data catalogs and query open table format data'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

# DataLakeCatalog

The `DataLakeCatalog` database engine enables you to connect ClickHouse to external
data catalogs and query open table format data without the need for data duplication.
This transforms ClickHouse into a powerful query engine that works seamlessly with
your existing data lake infrastructure.

## Supported catalogs {#supported-catalogs}

The `DataLakeCatalog` engine supports the following data catalogs:

- **AWS Glue Catalog** - For Iceberg tables in AWS environments
- **Databricks Unity Catalog** - For Delta Lake and Iceberg tables
- **Hive Metastore** - Traditional Hadoop ecosystem catalog
- **REST Catalogs** - Any catalog supporting the Iceberg REST specification

## Creating a database {#creating-a-database}

You will need to enable the relevant settings below to use the `DataLakeCatalog` engine:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

Databases with the `DataLakeCatalog` engine can be created using the following syntax:

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

The following settings are supported:

| Setting                 | Description                                                                             |
|-------------------------|-----------------------------------------------------------------------------------------|
| `catalog_type`          | Type of catalog: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg) |
| `warehouse`             | The warehouse/database name to use in the catalog.                                      |
| `catalog_credential`    | Authentication credential for the catalog (e.g., API key or token)                      |
| `auth_header`           | Custom HTTP header for authentication with the catalog service                          |
| `auth_scope`            | OAuth2 scope for authentication (if using OAuth)                                        |
| `storage_endpoint`      | Endpoint URL for the underlying storage                                                 |
| `oauth_server_uri`      | URI of the OAuth2 authorization server for authentication                               |
| `vended_credentials`    | Boolean indicating whether to use vended credentials from the catalog (supports AWS S3 and Azure ADLS Gen2) |
| `aws_access_key_id`     | AWS access key ID for S3/Glue access (if not using vended credentials)                  |
| `aws_secret_access_key` | AWS secret access key for S3/Glue access (if not using vended credentials)              |
| `region`                | AWS region for the service (e.g., `us-east-1`)                                          |
| `dlf_access_key_id`     | Access key ID for DLF access                                                            |
| `dlf_access_key_secret` | Access key Secret for DLF access                                                        |

## Creating tables {#creating-tables}

An Iceberg table in a `DataLakeCatalog` database can be created directly from ClickHouse.
The table name must be quoted with backticks and include the namespace separated by a dot:

```sql
CREATE TABLE catalog_db.`namespace.table_name`
(
    id Int64,
    name String,
    value Float64
)
PARTITION BY id
ORDER BY name
SETTINGS allow_database_iceberg = 1;
```

You can also create an Iceberg table that inherits the schema of an existing table:

```sql
CREATE TABLE catalog_db.`namespace.table_name`
AS other_db.source_table
SETTINGS allow_database_iceberg = 1;
```

## Dropping tables {#dropping-tables}

Tables can be dropped from a `DataLakeCatalog` database.
`DROP TABLE` sends a delete request to the remote catalog, which removes
the table entry from the catalog.

```sql
DROP TABLE catalog_db.`namespace.table_name`
```

By default, ClickHouse does not request the catalog to delete the underlying data. In order to do it, use the `database_iceberg_purge_on_drop` setting:

```sql
DROP TABLE catalog_db.`namespace.table_name`
SETTINGS database_iceberg_purge_on_drop = 1
```

:::note
Whether data files are actually deleted depends on the catalog itself.
The `purgeRequested` flag is sent to the catalog, but the catalog may choose to ignore it.
:::

## Examples {#examples}

See below sections for examples of using the `DataLakeCatalog` engine:

* [Unity Catalog](/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  Can be used by enabling `allow_experimental_database_iceberg` or `allow_database_iceberg`.
```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
   catalog_type = 'onelake',
   warehouse = warehouse,
   onelake_tenant_id = tenant_id,
   oauth_server_uri = server_uri,
   auth_scope = auth_scope,
   onelake_client_id = client_id,
   onelake_client_secret = client_secret;
SHOW TABLES IN databse_name;
SELECT count() from database_name.table_name;
```