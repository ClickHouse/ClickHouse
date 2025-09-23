---
description: 'This engine provides a read-only integration with existing Apache Hudi
  tables in Amazon S3.'
sidebar_label: 'Hudi'
sidebar_position: 86
slug: /engines/table-engines/integrations/hudi
title: 'Hudi Table Engine'
---

# Hudi Table Engine

This engine provides a read-only integration with existing Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Create Table {#create-table}

Note that the Hudi table must already exist in S3, this command does not take DDL parameters to create a new table.

```sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,])
```

**Engine parameters**

- `url` — Bucket url with the path to an existing Hudi table.
- `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file.

Engine parameters can be specified using [Named Collections](/operations/named-collections.md).

**Example**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

Using named collections:

```xml
<clickhouse>
    <named_collections>
        <hudi_conf>
            <url>http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/</url>
            <access_key_id>ABC123<access_key_id>
            <secret_access_key>Abc+123</secret_access_key>
        </hudi_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE hudi_table ENGINE=Hudi(hudi_conf, filename = 'test_table')
```

## See also {#see-also}

- [hudi table function](/sql-reference/table-functions/hudi.md)
