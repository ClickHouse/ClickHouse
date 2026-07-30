---
description: 'The DataLakeCatalog database engine enables you to connect ClickHouse to external data catalogs and query open table format data'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

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
| `catalog_type`          | Type of catalog: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`, `onelake` (Iceberg), `delta_sharing` (Iceberg, flat namespaces) |
| `warehouse`             | The warehouse/database name to use in the catalog.                                      |
| `catalog_credential`    | Authentication credential for the catalog (e.g., API key or token)                      |
| `auth_header`           | Custom HTTP header for authentication with the catalog service                          |
| `auth_scope`            | OAuth2 scope for authentication (if using OAuth)                                        |
| `storage_endpoint`      | Endpoint URL for the underlying storage                                                 |
| `default_base_location` | Base URI for new tables when the catalog does not report `default-base-location`. New tables are placed under `<default_base_location>/<namespace>/<table>` (e.g. `s3://warehouse/data`) |
| `oauth_server_uri`      | URI of the OAuth2 authorization server for authentication                               |
| `vended_credentials`    | Boolean indicating whether to use vended credentials from the catalog (supports AWS S3 and Azure ADLS Gen2) |
| `aws_access_key_id`     | AWS access key ID for S3/Glue access (if not using vended credentials)                  |
| `aws_secret_access_key` | AWS secret access key for S3/Glue access (if not using vended credentials)              |
| `aws_role_arn`          | ARN of the IAM role to assume for AWS/Glue access. When set, ClickHouse uses AWS STS `AssumeRole` with base credentials from `aws_access_key_id` and `aws_secret_access_key` when both are provided. If they are omitted, ClickHouse can use the default AWS credential chain only when `s3_allow_server_credentials_in_user_queries` is enabled; otherwise the request is rejected. |
| `aws_role_session_name` | Session name used for the AWS STS `AssumeRole` call. Optional; a default session name is used if not set. |
| `aws_external_id`       | External ID passed to AWS STS `AssumeRole`, matching the `sts:ExternalId` condition on the role's trust policy. Use this when the role is owned by a third party (for example, ClickHouse Cloud). |
| `region`                | AWS region for the service (e.g., `us-east-1`)                                          |
| `dlf_access_key_id`     | Access key ID for DLF access                                                            |
| `dlf_access_key_secret` | Access key Secret for DLF access                                                        |
| `force_add_bucket`      | When constructing object-storage URLs from the catalog-provided table location and `storage_endpoint`, prepend the bucket/container name even if the endpoint already contains it. Default: `false`. Set to `true` for catalogs that hand back paths without the bucket and require it to be added at the URL-construction step (Polaris-style paths). |

## Creating tables {#creating-tables}

An Iceberg table in a `DataLakeCatalog` database can be created directly from ClickHouse.

:::note
`CREATE TABLE` and `DROP TABLE` require a catalog that can perform catalog mutations. They are supported
for Iceberg REST catalogs (including OneLake, BigLake, and Delta Sharing) and for the AWS Glue catalog.
Other catalog types (Unity, Hive Metastore, Paimon REST) are read-only and reject these statements.
:::

The location of a newly created table comes from `default_base_location` (a full `s3://bucket/prefix`) when
set, otherwise the bucket is derived from `storage_endpoint`. With `storage_uri_style = 'virtual_hosted'` the
bucket cannot be derived from the endpoint unambiguously, so `default_base_location` is required for
`CREATE TABLE`.

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

Iceberg accepts only a fixed set of partition transforms, so `PARTITION BY`
must use one of the following expressions:

| Expression                    | Iceberg transform |
|-------------------------------|-------------------|
| `<column>`                    | `identity`        |
| `toYearNumSinceEpoch(<col>)`  | `year`            |
| `toMonthNumSinceEpoch(<col>)` | `month`           |
| `toRelativeDayNum(<col>)`     | `day`             |
| `toRelativeHourNum(<col>)`    | `hour`            |
| `icebergTruncate(N, <col>)`   | `truncate[N]`     |
| `icebergBucket(N, <col>)`     | `bucket[N]`       |

Composite partitioning is supported via `PARTITION BY (expr1, expr2, ...)`.
Other expressions (e.g. `toYYYYMM`, `intDiv`) are rejected at `CREATE TABLE`.

Only the column names and types, `PARTITION BY`, and `ORDER BY` are persisted into the Iceberg
table metadata. Anything else — the storage clauses `PRIMARY KEY`, `SAMPLE BY`, `TTL`, and
`UNIQUE KEY`; indices, constraints, and projections; and the column modifiers `DEFAULT`,
`MATERIALIZED`, `ALIAS`, `EPHEMERAL`, `COMMENT`, `CODEC`, `TTL`, `STATISTICS`, and `SETTINGS` —
is rejected rather than silently dropped. This applies both with and without an explicit
`ENGINE` clause. Engine `SETTINGS` are accepted only together with an explicit Iceberg engine,
where they are the engine's storage settings (e.g. `iceberg_format_version`).

You can also create an Iceberg table that inherits the schema of an existing table:

```sql
CREATE TABLE catalog_db.`namespace.table_name`
AS other_db.source_table
SETTINGS allow_database_iceberg = 1;
```

If the source table's `PARTITION BY` and `ORDER BY` use only the expressions
listed above, they are copied into the new Iceberg table.

## Dropping tables {#dropping-tables}

Tables can be dropped from a `DataLakeCatalog` database.
`DROP TABLE` sends a delete request to the remote catalog, which removes
the table entry from the catalog.

```sql
DROP TABLE catalog_db.`namespace.table_name`
```

By default, ClickHouse does not request the catalog to delete the underlying data. In order to do it, use the `data_lake_delete_data_on_drop` setting:

```sql
DROP TABLE catalog_db.`namespace.table_name`
SETTINGS data_lake_delete_data_on_drop = 1
```

:::note
Whether data files are actually deleted depends on the catalog itself.
The `purgeRequested` flag is sent to the catalog, but the catalog may choose to ignore it.
For the Glue catalog, `DROP TABLE` only removes the catalog entry and does not delete the underlying data
files, so `DROP TABLE` with `data_lake_delete_data_on_drop = 1` is rejected instead of silently leaving the
data behind.
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
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```
To authenticate without sharing a client secret, set `onelake_bearer_token` to a pre-obtained bearer token (scoped to `https://storage.azure.com`) instead of `onelake_client_id`/`onelake_client_secret`. ClickHouse does not refresh the token, so the database must be recreated after it expires.
