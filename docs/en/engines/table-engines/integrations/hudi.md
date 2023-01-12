---
slug: /en/engines/table-engines/integrations/hudi
sidebar_label: Hudi
---

# Hudi Table Engine

This engine provides a read-only integration with existing Apache [Hudi](https://hudi.apache.org/) tables in Amazon S3.

## Create Table

Note that the Hudi table must already exist in S3, this command does not take DDL parameters to create a new table.

``` sql
CREATE TABLE hudi_table
    ENGINE = Hudi(url, [aws_access_key_id, aws_secret_access_key,])
```

**Engine parameters**

-   `url` — Bucket url with the path to an existing Hudi table.
-   `aws_access_key_id`, `aws_secret_access_key` - Long-term credentials for the [AWS](https://aws.amazon.com/) account user.  You can use these to authenticate your requests. Parameter is optional. If credentials are not specified, they are used from the configuration file. For more information see [Using S3 for Data Storage](../mergetree-family/mergetree.md#table_engine-mergetree-s3).

**Example**

```sql
CREATE TABLE hudi_table ENGINE=Hudi('http://mars-doc-test.s3.amazonaws.com/clickhouse-bucket-3/test_table/', 'ABC123', 'Abc+123')
```

## See also

-  [hudi table function](/docs/en/sql-reference/table-functions/hudi.md)

