---
description: 'This engine provides a read-only integration with existing Delta Lake
  tables in Amazon S3.'
sidebar_label: 'DeltaLake'
sidebar_position: 40
slug: /engines/table-engines/integrations/deltalake
title: 'DeltaLake table engine'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# DeltaLake table engine

This engine provides an integration with existing [Delta Lake](https://github.com/delta-io/delta) tables in S3, GCP and Azure storage and supports both reads and writes (from v25.10).

## Create a DeltaLake table {#create-table}

To create a DeltaLake table it must already exist in S3, GCP or Azure storage. The commands below do not take DDL parameters to create a new table.

<Tabs>
<TabItem value="S3" label="S3" default>

**Syntax**

```sql
CREATE TABLE table_name
ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,])
```

**Engine parameters**

- `url` — Bucket url with path to the existing Delta Lake table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.

Engine parameters can be specified using [Named Collections](/operations/named-collections.md).

**Example**

```sql
CREATE TABLE deltalake
ENGINE = DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <deltalake_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </deltalake_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE deltalake
ENGINE = DeltaLake(deltalake_conf, filename = 'test_table')
```
</TabItem>

<TabItem value="GCP" label="GCP" default>

GCS is accessed through the S3-compatible API using HMAC credentials.

**Syntax**

```sql
-- Using HTTPS URL (recommended)
CREATE TABLE table_name
ENGINE = DeltaLake('https://storage.googleapis.com/<bucket>/<path>/', '<hmac_key>', '<hmac_secret>')

-- Using gs:// scheme (auto-converted to HTTPS)
CREATE TABLE table_name
ENGINE = DeltaLake('gs://<bucket>/<path>/', '<hmac_key>', '<hmac_secret>')
```

**Arguments**

- `url` — GCS bucket URL to the Delta Lake table. Must use `https://storage.googleapis.com/<bucket>/<path>/`
   format (the GCS XML API endpoint), or `gs://<bucket>/<path>/` which is auto-converted.
- `hmac_key` — GCS HMAC access key ID. Create via Google Cloud Console → Cloud Storage → Settings → Interoperability.
- `hmac_secret` — GCS HMAC secret key.
   
</TabItem>

<TabItem value="Azure" label="Azure" default>

**Syntax**

```sql
-- With connection string
CREATE TABLE table_name
ENGINE = DeltaLake(connection_string, container_name, blobpath [, format] [, compression])

-- With storage account URL and credentials
CREATE TABLE table_name
ENGINE = DeltaLake(storage_account_url, container_name, blobpath, account_name, account_key [,
format] [, compression])

-- With named collection (from tests)
CREATE TABLE table_name
ENGINE = DeltaLake(named_collection, container = 'container_name', storage_account_url = 'url',
blob_path = '/path/')
```

**Arguments**

- `connection_string` — Azure connection string
- `storage_account_url` — Azure storage account URL (e.g., https://account.blob.core.windows.net)
- `container_name` — Azure container name
- `blobpath` — Path to the Delta Lake table within the container
- `account_name` — Azure storage account name
- `account_key` — Azure storage account key

</TabItem>
</Tabs>

## Write data using a DeltaLake table {#insert-data}

Once you have created a table using the DeltaLake table engine, you can insert data into it with:

```sql
SET allow_experimental_delta_lake_writes = 1;

INSERT INTO deltalake(id, firstname, lastname, gender, age)
VALUES (1, 'John', 'Smith', 'M', 32);
```

:::note
Writing using the table engine is supported only through delta kernel.
Writes to Azure are not yet supported.
:::

### Data cache {#data-cache}

The `DeltaLake` table engine and table function support data caching, the same as `S3`, `AzureBlobStorage`, `HDFS` storages. See ["S3 table engine"](../../../engines/table-engines/integrations/s3.md#data-cache) for more details.

## See also {#see-also}

- [deltaLake table function](../../../sql-reference/table-functions/deltalake.md)
