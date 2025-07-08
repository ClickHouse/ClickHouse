---
description: 'The DataLakeCatalog database engine enables you to connect ClickHouse to external data catalogs and query open table format data'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
---

# DataLakeCatalog

The `DataLakeCatalog` database engine enables you to connect ClickHouse to external
data catalogs and query open table format data without the need for data duplication.
This transforms ClickHouse into a powerful query engine that works seamlessly with
your existing data lake infrastructure.

## Supported Catalogs {#supported-catalogs}

The `DataLakeCatalog` engine supports the following data catalogs:

- **AWS Glue Catalog** - For Iceberg tables in AWS environments
- **Databricks Unity Catalog** - For Delta Lake and Iceberg tables
- **Hive Metastore** - Traditional Hadoop ecosystem catalog
- **REST Catalogs** - Any catalog supporting the Iceberg REST specification

## Creating a Database {#creating-a-database}

You will need to enable the relevant settings below to use the `DataLakeCatalog` engine:

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
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

| Setting                 | Description                                                               |
|-------------------------|---------------------------------------------------------------------------|
| `catalog_type`          | Type of catalog: `glue`, `unity` (Delta), `rest` (Iceberg), `hive`        |
| `warehouse`             | The warehouse/database name to use in the catalog.                        |
| `catalog_credential`    | Authentication credential for the catalog (e.g., API key or token)        |
| `auth_header`           | Custom HTTP header for authentication with the catalog service            |
| `auth_scope`            | OAuth2 scope for authentication (if using OAuth)                          |
| `storage_endpoint`      | Endpoint URL for the underlying storage                                   |
| `oauth_server_uri`      | URI of the OAuth2 authorization server for authentication                 |
| `vended_credentials`    | Boolean indicating whether to use vended credentials (AWS-specific)       |
| `aws_access_key_id`     | AWS access key ID for S3/Glue access (if not using vended credentials)    |
| `aws_secret_access_key` | AWS secret access key for S3/Glue access (if not using vended credentials) |
| `region`                | AWS region for the service (e.g., `us-east-1`)                             |

## Examples {#examples}

See below pages for examples of using the `DataLakeCatalog` engine:

* [Unity Catalog](/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/use-cases/data-lake/glue-catalog)
