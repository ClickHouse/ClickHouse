---
slug: /en/sql-reference/table-functions/iceberg
sidebar_position: 90
sidebar_label: iceberg
---

# iceberg Table Function

Provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS or locally stored.

## Syntax

``` sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

## Arguments

Description of the arguments coincides with description of arguments in table functions `s3`, `azureBlobStorage`, `HDFS` and `file` correspondingly.
`format` stands for the format of data files in the Iceberg table.

**Returned value**
A table with the specified structure for reading data in the specified Iceberg table.

**Example**

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse currently supports reading v1 and v2 of the Iceberg format via the `icebergS3`, `icebergAzure`, `icebergHDFS` and `icebergLocal` table functions and `IcebergS3`, `icebergAzure`, `IcebergHDFS` and `IcebergLocal` table engines.
:::

## Defining a named collection

Here is an example of configuring a named collection for storing the URL and credentials:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test<access_key_id>
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

**Schema Evolution**
At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  
* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P. 

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

**Aliases**

Table function `iceberg` is an alias to `icebergS3` now.

**See Also**

- [Iceberg engine](/docs/en/engines/table-engines/integrations/iceberg.md)
- [Iceberg cluster table function](/docs/en/sql-reference/table-functions/icebergCluster.md)
