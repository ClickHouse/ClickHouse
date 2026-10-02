---
description: 'Documentation for Data Lakes'
sidebar_label: 'Data Lakes'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'Data Lakes'
doc_type: 'reference'
---

In this section, we will take a look at ClickHouse's support for Data Lakes.
ClickHouse supports many of the most popular table formats and data catalogs, including Iceberg, Delta Lake, Hudi, AWS Glue, REST Catalog, Unity Catalog and Microsoft OneLake.

# Open table formats {#open-table-formats}

## Iceberg {#iceberg}

See [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg) which supports reading from Amazon S3 and S3-compatible services, HDFS, Azure and local file systems. [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) is the distributed variant of the `iceberg` function.

## Delta Lake {#delta-lake}

See [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake) which supports reading from Amazon S3 and S3-compatible services, Azure and local file systems. [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) is the distributed variant of the `deltaLake` function.

## Hudi {#hudi}

See [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi) which supports reading from Amazon S3 and S3-compatible services. [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) is the distributed variant of the `hudi` function.

# Data catalogs {#data-catalogs}

## AWS Glue {#aws-glue}

AWS Glue Data Catalog can be used with Iceberg tables. You can use it with the `iceberg` table engine, or with the [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) database engine.

## Iceberg REST Catalog {#iceberg-rest-catalog}

The Iceberg REST Catalog can be used with Iceberg tables. You can use it with the `iceberg` table engine, or with the [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) database engine.

## Unity Catalog {#unity-catalog}

Unity Catalog can be used with both Delta Lake and Iceberg tables. You can use it with the `iceberg` or `deltaLake` table engines, or with the [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) database engine.

## Microsoft OneLake {#microsoft-onelake}

Microsoft OneLake can be used with both Delta Lake and Iceberg tables. You can use it with the [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) database engine.
