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

Catalog integration is done through the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine. Settings such as `catalog_type` are database-level settings and are not available on bare `Iceberg*` / `DeltaLake*` table engines.

## AWS Glue {#aws-glue}

AWS Glue Data Catalog can be used with Iceberg tables via the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine. See the [Glue catalog guide](/use-cases/data-lake/glue-catalog).

## Iceberg REST Catalog {#iceberg-rest-catalog}

The Iceberg REST Catalog can be used with Iceberg tables via the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine. See the [REST catalog guide](/use-cases/data-lake/rest-catalog).

## Unity Catalog {#unity-catalog}

Unity Catalog can be used with both Delta Lake and Iceberg tables via the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine. See the [Unity catalog guide](/use-cases/data-lake/unity-catalog).

## Microsoft OneLake {#microsoft-onelake}

Microsoft OneLake can be used with both Delta Lake and Iceberg tables via the [`DataLakeCatalog`](/engines/database-engines/datalakecatalog) database engine. See the [OneLake catalog guide](/use-cases/data-lake/onelake-catalog).
