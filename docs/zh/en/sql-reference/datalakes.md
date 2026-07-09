---
description: '数据湖文档'
sidebar_label: '数据湖'
sidebar_position: 2
slug: /sql-reference/datalakes
title: '数据湖'
doc_type: 'reference'
---

本节将介绍 ClickHouse 对数据湖的支持。
ClickHouse 支持许多最常用的表格式和数据目录，包括 Iceberg、Delta Lake、Hudi、AWS Glue、REST Catalog、Unity Catalog 和 Microsoft OneLake。

<div id="open-table-formats">
  # 开放表存储格式
</div>

<div id="iceberg">
  ## Iceberg
</div>

参见 [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg)，它支持从亚马逊 S3、兼容 S3 的服务、HDFS、Azure 以及本地文件系统中读取数据。[icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) 是 `iceberg` 函数的分布式版本。

<div id="delta-lake">
  ## Delta Lake
</div>

请参阅 [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake)，该函数支持从亚马逊 S3、兼容 S3 的服务、Azure 和本地文件系统读取数据。[deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) 是 `deltaLake` 函数的分布式版本。

<div id="hudi">
  ## Hudi
</div>

请参见 [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi)，该函数支持从亚马逊 S3 和兼容 S3 的服务读取数据。[hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) 是 `hudi` 函数的分布式版本。

<div id="data-catalogs">
  # 数据目录
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

AWS Glue 数据目录可与 Iceberg 表一起使用。你可以将其与 `iceberg` 表引擎搭配使用，也可以与 [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) 数据库引擎搭配使用。

<div id="iceberg-rest-catalog">
  ## Iceberg REST Catalog
</div>

Iceberg REST Catalog 可用于 Iceberg 表。你可以将其与 `iceberg` 表引擎配合使用，也可以与 [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) 数据库引擎配合使用。

<div id="unity-catalog">
  ## Unity Catalog
</div>

Unity Catalog 可用于 Delta Lake 和 Iceberg 表。你可以将其与 `iceberg` 或 `deltaLake` 表引擎一起使用，也可以与 [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) 数据库引擎一起使用。

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

Microsoft OneLake 可同时用于 Delta Lake 和 Iceberg 表，也可搭配 [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) 数据库引擎使用。