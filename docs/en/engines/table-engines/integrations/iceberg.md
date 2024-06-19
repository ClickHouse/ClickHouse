---
slug: /en/engines/table-engines/integrations/iceberg
sidebar_position: 90
sidebar_label: Iceberg
---

# Iceberg Table Engine

This engine provides a read-only integration with existing Apache [Iceberg](https://iceberg.apache.org/) tables in Amazon S3.

## Create Table

Note that the Iceberg table must already exist in S3, this command does not take DDL parameters to create a new table.

``` sql
CREATE TABLE iceberg_table
    ENGINE = Iceberg(url, [aws_access_key_id, aws_secret_access_key,])
```

**Engine parameters**

- `url` — url with the path to an existing Iceberg table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.

Engine parameters can be specified using [Named Collections](../../../operations/named-collections.md)

**Example**

```sql
CREATE TABLE iceberg_table ENGINE=Iceberg('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
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
CREATE TABLE iceberg_table ENGINE=Iceberg(iceberg_conf, filename = 'test_table')
```

## See also

- [iceberg table function](/docs/en/sql-reference/table-functions/iceberg.md)
