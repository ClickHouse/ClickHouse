---
slug: /en/engines/table-engines/integrations/iceberg
sidebar_position: 90
sidebar_label: Iceberg
---

# Iceberg Table Engine

:::warning 
We recommend using the [Iceberg Table Function](/docs/en/sql-reference/table-functions/iceberg.md) for working with Iceberg data in ClickHouse. The Iceberg Table Function currently provides sufficient functionality, offering a partial read-only interface for Iceberg tables.

The Iceberg Table Engine is available but may have limitations. ClickHouse wasn't originally designed to support tables with externally changing schemas, which can affect the functionality of the Iceberg Table Engine. As a result, some features that work with regular tables may be unavailable or may not function correctly, especially when using the old analyzer.

For optimal compatibility, we suggest using the Iceberg Table Function while we continue to improve support for the Iceberg Table Engine.
:::

This engine provides a read-only integration with existing Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3, Azure, HDFS and locally stored tables.

## Create Table

Note that the Iceberg table must already exist in the storage, this command does not take DDL parameters to create a new table.

``` sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

**Engine arguments**

Description of the arguments coincides with description of arguments in engines `S3`, `AzureBlobStorage`, `HDFS` and `File` correspondingly.
`format` stands for the format of data files in the Iceberg table.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

**Example**

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Using named collections:

``` xml
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

**Aliases**


Table engine `Iceberg` is an alias to `IcebergS3` now.

**Schema Evolution**
At the moment, with the help of CH, you can read iceberg tables, the schema of which has changed over time. We currently support reading tables where columns have been added and removed, and their order has changed. You can also change a column where a value is required to one where NULL is allowed. Additionally, we support permitted type casting for simple types, namely:  
* int -> long
* float -> double
* decimal(P, S) -> decimal(P', S) where P' > P. 

Currently, it is not possible to change nested structures or the types of elements within arrays and maps.

To read a table where the schema has changed after its creation with dynamic schema inference, set allow_dynamic_metadata_for_data_lakes = true when creating the table.

### Data cache {#data-cache}

`Iceberg` table engine and table function support data caching same as `S3`, `AzureBlobStorage`, `HDFS` storages. See [here](../../../engines/table-engines/integrations/s3.md#data-cache).

## See also

- [iceberg table function](/docs/en/sql-reference/table-functions/iceberg.md)
