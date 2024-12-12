---
slug: /en/sql-reference/table-functions/iceberg
sidebar_position: 90
sidebar_label: iceberg
---

# iceberg Table Function

Provides a read-only table-like interface to Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3.

## Syntax

``` sql
iceberg(url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure])
```

## Arguments

- `url` — Bucket url with the path to an existing Iceberg table in S3.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. These parameters are optional. If credentials are not specified, they are used from the ClickHouse configuration. For more information see [Using S3 for Data Storage](/docs/en/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3).
- `format` — The [format](/docs/en/interfaces/formats.md/#formats) of the file. By default `Parquet` is used.
- `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

Engine parameters can be specified using [Named Collections](/docs/en/operations/named-collections.md).

**Returned value**

A table with the specified structure for reading data in the specified Iceberg table in S3.

**Example**

```sql
SELECT * FROM iceberg('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
ClickHouse currently supports reading v1 (v2 support is coming soon!) of the Iceberg format via the `iceberg` table function and `Iceberg` table engine.
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
SELECT * FROM iceberg(iceberg_conf, filename = 'test_table')
DESCRIBE iceberg(iceberg_conf, filename = 'test_table')
```

**See Also**

- [Iceberg engine](/docs/en/engines/table-engines/integrations/iceberg.md)
