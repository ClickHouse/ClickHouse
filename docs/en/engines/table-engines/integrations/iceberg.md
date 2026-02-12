---
description: 'This engine provides a read-only integration with existing Apache Iceberg
  tables in Amazon S3, Azure, HDFS and locally stored tables.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Iceberg table engine'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import S3Parameters from './snippets/_s3_parameters.md';
import AzureBlobStorageParameters from './snippets/_azure_blob_storage_parameters.md';
import HDFSParameters from './snippets/_hdfs_parameters.md';

# Iceberg table engine {#iceberg-table-engine}

:::warning
We recommend using the [Iceberg table function](/sql-reference/table-functions/iceberg) for working with Iceberg data in ClickHouse.
The Iceberg Table Function currently provides sufficient functionality, offering a partial read-only interface for Iceberg tables.

The Iceberg Table Engine is available but may have limitations.
ClickHouse wasn't originally designed to support tables with externally changing schemas, which can affect the functionality of the Iceberg Table Engine. As a result, some features that work with regular tables may be unavailable or may not function correctly, especially when using the old analyzer.

For optimal compatibility, we suggest using the Iceberg Table Function while we continue to improve support for the Iceberg Table Engine.
:::

This engine provides a read-only integration with existing Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.

## Create a table {#create-table}

Note that the Iceberg table must already exist in the storage, this command does not take DDL parameters to create a new table.

**Table engine `Iceberg` is an alias to `IcebergS3` now.**

<Tabs>
<TabItem value="S3" label="S3" default>

```sql
CREATE TABLE iceberg_table_s3
ENGINE = IcebergS3(path[, NOSIGN | aws_access_key_id, aws_secret_access_key, [session_token]], format[,compression])
```

<br/>
<details>
<summary>See argument descriptions</summary>
<S3Parameters/>
</details>

:::note
`format` stands for the format of data files in the Iceberg table.
:::

<br/>
</TabItem>

<TabItem value="Azure" label="Azure">

```sql
CREATE TABLE iceberg_table_azure
ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
```

<details>
<summary>See argument descriptions</summary>
<AzureBlobStorageParameters/>
</details>

:::note
`format` stands for the format of data files in the Iceberg table.
:::
<br/>
</TabItem>

<TabItem value="HDFS" label="HDFS">

```sql
CREATE TABLE iceberg_table_hdfs
ENGINE = IcebergHDFS(URI[,format][,compression_method])
```

<details>
<summary>See argument descriptions</summary>
<HDFSParameters/>
</details>

:::note
`format` stands for the format of data files in the Iceberg table.
:::

</TabItem>

<TabItem value="Local" label="Local storage">

```sql
CREATE TABLE iceberg_table_local
ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

| Parameter            | Description                                                                                                                                      |
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| `path_to_table`      | Path to the Iceberg table directory in the local filesystem.                                                                                     |
| `format`             | Specifies one of the available file formats. The available formats are listed in the [Formats](/sql-reference/formats#formats-overview) section. |
| `compression_method` | Compression method. Supported values: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Optional.                                           |

:::note
`format` stands for the format of data files in the Iceberg table.
:::
<br/>
</TabItem>
</Tabs>

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

## Examples {#examples}

<Tabs>
<TabItem value="S3" label="S3" default>

```sql
CREATE TABLE iceberg_table
ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

</TabItem>

<TabItem value="Azure" label="Azure">

```sql
CREATE TABLE iceberg_table
ENGINE=IcebergAzure('DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;', 'mycontainer', 'path/to/iceberg_table')
```

</TabItem>

<TabItem value="HDFS" label="HDFS">

```sql
CREATE TABLE iceberg_table
ENGINE=IcebergHDFS('hdfs://hdfs-namenode:9000/warehouse/iceberg_table')
```

</TabItem>

<TabItem value="Local" label="Local storage">

```sql
CREATE TABLE iceberg_table
ENGINE=IcebergLocal('/path/to/local/iceberg_table')
```

</TabItem>
</Tabs>

## Using named collections {#using-named-collections}

Here is an example of configuring a named collection for storing the connection details and credentials:

<Tabs>
<TabItem value="S3" label="S3" default>

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')
```

</TabItem>

<TabItem value="Azure" label="Azure">

```xml
<clickhouse>
    <named_collections>
        <iceberg_azure_conf>
            <connection_string>DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net</connection_string>
            <container_name>mycontainer</container_name>
        </iceberg_azure_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergAzure(iceberg_azure_conf, blobpath = 'path/to/iceberg_table')
```

</TabItem>

<TabItem value="HDFS" label="HDFS">

```xml
<clickhouse>
    <named_collections>
        <iceberg_hdfs_conf>
            <url>hdfs://hdfs-namenode:9000/warehouse/</url>
        </iceberg_hdfs_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergHDFS(iceberg_hdfs_conf, path_to_table = 'iceberg_table')
```

</TabItem>

<TabItem value="Local" label="Local storage">

```xml
<clickhouse>
    <named_collections>
        <iceberg_local_conf>
            <path>/path/to/tables/</path>
        </iceberg_local_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergLocal(iceberg_local_conf, path_to_table = 'iceberg_table')
```

</TabItem>
</Tabs>

## Features {#features}

The `iceberg` table engine offers much of the same functionality that the recommended `iceberg` table function does.
See the corresponding sections in the table function docs for more details:

| Feature                                                                                                              |
|----------------------------------------------------------------------------------------------------------------------|
| [Schema evolution](/sql-reference/table-functions/iceberg-schema-evolution)                                          |
| [Partition pruning](/sql-reference/table-functions/iceberg-query-optimization#partition-pruning)                     |
| [Time travel](/sql-reference/table-functions/iceberg-time-travel)                                                    |
| [Processing of tables with deleted rows](/sql-reference/table-functions/iceberg-processing-tables-with-deleted-rows) |
| [Metadata file resolution](/sql-reference/table-functions/iceberg-metadata-resolution)                               |
| [Data cache](/sql-reference/table-functions/iceberg-query-optimization#data-cache)                                   |
| [Metadata cache](/sql-reference/table-functions/iceberg-metadata-resolution#metadata-cache)                          |

## See also {#see-also}

- [iceberg table function](/sql-reference/table-functions/iceberg.md)