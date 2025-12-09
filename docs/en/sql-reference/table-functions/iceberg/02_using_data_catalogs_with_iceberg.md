---
description: 'How to use data catalogs with iceberg'
sidebar_label: 'Data catalogs'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg-data-catalogs
title: 'Using data catalogs with iceberg'
doc_type: 'reference'
---

# Using data catalogs with iceberg {#iceberg-writes-catalogs}

Iceberg tables can also be used with various data catalogs, such as:
- [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html)
- [Unity Catalog](https://www.unitycatalog.io/)

:::important
When using a catalog, most users will want to use the `DataLakeCatalog` database engine, which connects ClickHouse to your catalog to discover your tables.
You can use this database engine instead of manually creating individual tables with `IcebergS3` table engine.
:::

To use them, create a table with the `IcebergS3` engine and provide the necessary settings.

For example, using REST Catalog with MinIO storage:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
SETTINGS 
  storage_catalog_type="rest",
  storage_warehouse="demo",
  object_storage_endpoint="http://minio:9000/warehouse-rest",
  storage_region="us-east-1",
  storage_catalog_url="http://rest:8181/v1"
```

Or, using AWS Glue Data Catalog with S3:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
SETTINGS 
  storage_catalog_type = 'glue',
  storage_warehouse = 'my_database',
  object_storage_endpoint = 's3://my-data-bucket/',
  storage_region = 'us-east-1',
  storage_catalog_url = 'https://glue.us-east-1.amazonaws.com/iceberg/v1'
```

For more examples, see [data lake guides](/use-cases/data-lake).