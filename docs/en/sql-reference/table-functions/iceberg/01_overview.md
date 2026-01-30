---
description: 'Provides a read-only table-like interface to Apache Iceberg tables in Amazon S3, Azure, HDFS or local storage.'
sidebar_label: 'Overview'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import S3Arguments from '../snippets/_s3_arguments.md'
import AzureArguments from '../snippets/_azure_arguments.md'
import HDFSArguments from '../snippets/_hdfs_arguments.md'
import FileArguments from '../snippets/_file_arguments.md'

# iceberg table function {#iceberg-table-function}

The `iceberg` table function provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS or local storage.

:::tip
The `iceberg` table function is the recommended way of working with iceberg data in ClickHouse.
:::

## Syntax and arguments {#syntax}

**Table function `iceberg` is an alias to `icebergS3` now.**

<Tabs>
<TabItem value="S3" label="S3" default>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3(named_collection[, option=value [,..]])
```

The arguments for the `icebergS3` table function are the same as for the [S3](/sql-reference/table-functions/s3) table function.

<br/>
<details>
<summary>See argument descriptions</summary>
<S3Arguments/>
</details>

</TabItem>

<TabItem value="Azure" label="Azure" default>

```sql
icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])
```

The arguments for the `icebergAzure` table function are the same as for the [`azureBlobStorage`](/sql-reference/table-functions/azureBlobStorage) table function.

<br/>
<details>
<summary>See argument descriptions</summary>
<AzureArguments/>
</details>

</TabItem>

<TabItem value="HDFS" label="HDFS" default>

```sql
icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])
```

The arguments for the `icebergHDFS` table function are the same as for the [`hdfs`](/sql-reference/table-functions/hdfs) table function.

<br/>
<details>
<summary>See argument descriptions</summary>
<HDFSArguments/>
</details>

</TabItem>

<TabItem value="Local" label="Local storage" default>

```sql
icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

The arguments for the `icebergLocal` table function are the same as for the [`file`](/sql-reference/table-functions/file) table function.

<br/>
<details>
<summary>See argument descriptions</summary>
<FileArguments/>
</details>

</TabItem>
</Tabs>

## Returned value {#returned-value}

Returns a table with the specified structure for reading data from the remote or local iceberg table.

## Examples {#example}

<Tabs>
<TabItem value="S3" label="S3" default>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

</TabItem>

<TabItem value="Azure" label="Azure">

```sql
SELECT * FROM icebergAzure('DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;', 'mycontainer', 'path/to/iceberg_table')
```

</TabItem>

<TabItem value="HDFS" label="HDFS">

```sql
SELECT * FROM icebergHDFS('hdfs://hdfs-namenode:9000/warehouse/iceberg_table')
```

</TabItem>

<TabItem value="Local" label="Local storage">

```sql
SELECT * FROM icebergLocal('/path/to/local/iceberg_table')
```

</TabItem>
</Tabs>

<br/>
:::important
ClickHouse currently supports reading v1 and v2 of the Iceberg format via the `icebergS3`, `icebergAzure`, `icebergHDFS` and `icebergLocal` table functions and `IcebergS3`, `IcebergAzure`, `IcebergHDFS` and `IcebergLocal` table engines.
:::

## Defining a named collection {#defining-a-named-collection}

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
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

</TabItem>

<TabItem value="Azure" label="Azure">

```xml
<clickhouse>
    <named_collections>
        <iceberg_azure_conf>
            <!--<storage_account_url>https://myaccount.blob.core.windows.net</storage_account_url>-->
            <connection_string>DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey;EndpointSuffix=core.windows.net</connection_string>
            <container_name>mycontainer</container_name>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_azure_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergAzure(iceberg_azure_conf, blobpath = 'path/to/iceberg_table');
DESCRIBE icebergAzure(iceberg_azure_conf, blobpath = 'path/to/iceberg_table');
```

</TabItem>

<TabItem value="HDFS" label="HDFS">

```xml
<clickhouse>
    <named_collections>
        <iceberg_hdfs_conf>
            <url>hdfs://hdfs-namenode:9000/warehouse/</url>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_hdfs_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergHDFS(iceberg_hdfs_conf, path_to_table = 'iceberg_table');
DESCRIBE icebergHDFS(iceberg_hdfs_conf, path_to_table = 'iceberg_table');
```

</TabItem>

<TabItem value="Local" label="Local storage">

```xml
<clickhouse>
    <named_collections>
        <iceberg_local_conf>
            <path>/path/to/tables/</path>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_local_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergLocal(iceberg_local_conf, path_to_table = 'iceberg_table');
DESCRIBE icebergLocal(iceberg_local_conf, path_to_table = 'iceberg_table');
```

</TabItem>
</Tabs>

## Virtual columns {#virtual-columns}

| Column  | Type                     | Description                                                                 |
|---------|--------------------------|-----------------------------------------------------------------------------|
| `_path` | `LowCardinality(String)` | Path to the file                                                            |
| `_file` | `LowCardinality(String)` | Name of the file                                                            |
| `_size` | `Nullable(UInt64)`       | Size of the file in bytes. If the file size is unknown, the value is `NULL` |
| `_time` | `Nullable(DateTime)`     | Last modified time of the file. If the time is unknown, the value is `NULL` |
| `_etag` | `LowCardinality(String)` | The etag of the file. If the etag is unknown, the value is `NULL`           |

## See Also {#see-also}

* [Iceberg engine](/engines/table-engines/integrations/iceberg.md)
* [Iceberg cluster table function](/sql-reference/table-functions/icebergCluster.md)
