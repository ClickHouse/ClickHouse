---
slug: /en/engines/database-engines/iceberg
sidebar_position: 30
sidebar_label: Iceberg
---
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# Iceberg
<ExperimentalBadge/>

This database engine provides integration with external catalogs. The initial implementation supports Iceberg REST catalogs, with plans to extend support to other open table formats such as Delta Lake.

:::note
This is an experimental feature currently under development and may change in a backward-incompatible manner. You can enable it by setting:

``` sql
SET allow_experimental_database_iceberg=1;
```
:::

## Creating a connection {#creating-a-connection}

``` sql
CREATE DATABASE catalog_name
ENGINE = Iceberg(endpoint)
[SETTINGS]
[catalog_type = 'rest',]
[catalog_credential = 'token',]  
[warehouse = 'warehouse_name',]
[oauth_server_uri = 'URL',]
[auth_scope = 'all-apis,sql']
```

**Engine Parameters**

- `endpoint`: Endpoint of the catalog

## Using the catalog {#using-the-catalog}
ClickHouse does not yet support an arbitrary number of namespaces. To avoid issues, enclose namespaces and table names in backticks when querying them. For example, if your table `my_table` is behind a catalog called `my_catalog` in the namespace `namespace1`, you should access it as follows:

```sql
SELECT * FROM my_catalog.`namespace1.my_table` LIMIT 10;
```

Once the connection is created, you can interact with tables in your catalog as if they were native ClickHouse tables.

```sql
SHOW TABLES FROM my_catalog;
SHOW CREATE TABLE my_catalog.`namespace1.table_name`;
SELECT * FROM my_catalog.`namespace1.my_table` LIMIT 10;
```

## Settings {#settings}

### `catalog_type`

Type of catalog that is going to be used.

**Possible values:**

rest: To be used when using a rest catalog

- String

### `catalog_credential`

Credential used to connect to the catalog.

- String

### `warehouse`

Name of the warehouse to be used in the catalog.

- String

### `oauth_server_uri`

URI of the OAuth server, in case a different URI is needed, for Unity for example.

- String

### `auth_scope`

Granted Oauth scope. String of comma separated scope: `all-api,sql` for example.

- String
