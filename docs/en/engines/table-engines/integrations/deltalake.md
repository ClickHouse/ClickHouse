---
slug: /en/engines/table-engines/integrations/deltalake
sidebar_position: 40
sidebar_label: DeltaLake
---

# DeltaLake Table Engine

This engine provides a read-only integration with existing [Delta Lake](https://github.com/delta-io/delta) tables in Amazon S3.

## Create Table

Note that the Delta Lake table must already exist in S3, this command does not take DDL parameters to create a new table.

``` sql
CREATE TABLE deltalake
    ENGINE = DeltaLake(url, [aws_access_key_id, aws_secret_access_key,])
```

**Engine parameters**

- `url` — Bucket url with path to the existing Delta Lake table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.

Engine parameters can be specified using [Named Collections](/docs/en/operations/named-collections.md).

**Example**

```sql
CREATE TABLE deltalake ENGINE=DeltaLake('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Using named collections:

``` xml
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
CREATE TABLE deltalake ENGINE=DeltaLake(deltalake_conf, filename = 'test_table')
```

### Data cache {#data-cache}

`Iceberg` table engine and table function support data caching same as `S3`, `AzureBlobStorage`, `HDFS` storages. See [here](../../../engines/table-engines/integrations/s3.md#data-cache).

## See also

- [deltaLake table function](../../../sql-reference/table-functions/deltalake.md)
