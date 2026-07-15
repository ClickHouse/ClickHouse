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
| `oauth_server_uri`      | URI of the OAuth2 authorization server for authentication                               |
| `vended_credentials`    | Boolean indicating whether to use vended credentials from the catalog (supports AWS S3 and Azure ADLS Gen2) |
| `aws_access_key_id`     | AWS access key ID for S3/Glue access (if not using vended credentials)                  |
| `aws_secret_access_key` | AWS secret access key for S3/Glue access (if not using vended credentials)              |
| `region`                | AWS region for the service (e.g., `us-east-1`)                                          |
| `dlf_access_key_id`     | Access key ID for DLF access                                                            |
| `dlf_access_key_secret` | Access key Secret for DLF access                                                        |
| `force_add_bucket`      | When constructing object-storage URLs from the catalog-provided table location and `storage_endpoint`, prepend the bucket/container name even if the endpoint already contains it. Default: `false`. Set to `true` for catalogs that hand back paths without the bucket and require it to be added at the URL-construction step (Polaris-style paths). |

## Referencing tables in namespaces {#referencing-tables-in-namespaces}

:::note
Table namespaces are an experimental feature, disabled by default. Enable them
with `SET allow_experimental_table_namespaces = 1`. While the setting is off,
everything below is unavailable and multipart paths are rejected (`SHOW COLUMNS`
and `SHOW INDEXES` keep their historical interpretation of a multipart operand);
the quoted form `` catalog_name.`namespace.table` `` always works.
:::

Data lake catalogs organize tables into namespaces, which can be nested to
several levels. A table's fully-qualified name inside the catalog is therefore
`namespace.table` (or `namespace1.namespace2.table` for a nested namespace),
and it can always be referenced by quoting that name as a single identifier:

```sql
SELECT * FROM catalog_name.`namespace.table`;
```

With `allow_experimental_table_namespaces` enabled, you can also spell out
every part with dots. The rules are deterministic and do not depend on what
currently exists in any catalog:

- `a.b` always means database `a`, table `b`.
- `db.ns.table` (three or more parts) always means the table path `ns.table`
  inside database `db`:

```sql
-- Equivalent to catalog_name.`namespace.table`
SELECT * FROM catalog_name.namespace.table;

-- Equivalent to catalog_name.`namespace1.namespace2.table`
SELECT * FROM catalog_name.namespace1.namespace2.table;
```

This works for reads and introspection: `SELECT`, `INSERT`, `EXISTS TABLE`,
`DESCRIBE`, `SHOW CREATE TABLE`, `SHOW COLUMNS`, and `SHOW INDEXES`.

### Using a namespace as a scope {#using-a-namespace-as-a-scope}

You can select a namespace with `USE catalog_name.namespace` so that
unqualified table names are resolved within that namespace:

```sql
USE catalog_name.namespace;
-- Resolved as catalog_name.`namespace.table`
SELECT * FROM table;
```

`USE catalog_name.namespace` validates that the namespace exists in the
catalog and fails otherwise. While a namespace is selected, `currentDatabase`
still returns the physical database (`catalog_name`), and `SHOW TABLES` lists
only the direct children of the namespace, by their stored (namespace-qualified)
names. `SHOW TABLES FROM catalog_name.namespace` does the same without changing the
scope. The scope is cleared as soon as you switch to another database with
`USE`.

Because a two-part name always means `database.table`, writing
`namespace.table` without the catalog prefix does **not** resolve inside the
current database - use the full path or `USE catalog_name.namespace`.

While a namespace is selected, statements that do not support namespace
scoping - DDL (`CREATE`, `DROP`, `ALTER`, `RENAME`, `OPTIMIZE`, `TRUNCATE`),
`ON CLUSTER` queries, `BACKUP`/`RESTORE`, access-control statements, `SYSTEM`
commands, and similar - fail with an error instead of silently targeting the
database without the namespace. The same applies to unqualified parameterized
views, the one-argument `merge` table function, and to disabling
`allow_experimental_table_namespaces` itself. Switch to the plain database with
`USE catalog_name` and use quoted canonical names for those operations. The
selected scope also requires the analyzer (`enable_analyzer`, on by default).

A path component cannot contain a literal dot: a back-quoted component like
`` catalog_name.`a.b`.table `` is rejected, because after parsing it would be
indistinguishable from `catalog_name.a.b.table`. Catalog objects whose native
names contain literal dots remain visible in listings, but they cannot be
addressed through a multipart path - only through the quoted canonical name.

The same mechanism works for regular databases (`Atomic`, `Memory`), where a
dot inside a table name lexically defines a namespace: a table named
`ns.table` in database `db` can be addressed as `db.ns.table` or through
`USE db.ns`.

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
