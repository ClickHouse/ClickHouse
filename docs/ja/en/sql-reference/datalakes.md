---
description: 'データレイクに関するドキュメント'
sidebar_label: 'データレイク'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'データレイク'
doc_type: 'reference'
---

このセクションでは、ClickHouse のデータレイク対応について説明します。
ClickHouse は、Iceberg、Delta Lake、Hudi、AWS Glue、REST Catalog、Unity Catalog、Microsoft OneLake など、広く利用されているテーブルフォーマットやカタログを多数サポートしています。

<div id="open-table-formats">
  # オープンテーブルフォーマット
</div>

<div id="iceberg">
  ## Iceberg
</div>

Amazon S3、S3互換サービス、HDFS、Azure、ローカルファイルシステムからの読み取りをサポートする[iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg)を参照してください。[icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster)は、`iceberg`関数の分散版です。

<div id="delta-lake">
  ## Delta Lake
</div>

Amazon S3、S3互換サービス、Azure、ローカルファイルシステムからの読み取りをサポートする [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake) を参照してください。[deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) は、`deltaLake` 関数の分散版です。

<div id="hudi">
  ## Hudi
</div>

Amazon S3 および S3互換サービスからの読み取りをサポートする [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi) を参照してください。[hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) は、`hudi` 関数の分散版です。

<div id="data-catalogs">
  # データカタログ
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

AWS Glue Data Catalog は Iceberg テーブルと組み合わせて使用できます。`iceberg` テーブルエンジン、または [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) データベースエンジンで利用できます。

<div id="iceberg-rest-catalog">
  ## Iceberg REST Catalog
</div>

Iceberg REST Catalog は、Iceberg テーブルで使用できます。`iceberg` テーブルエンジンまたは [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) データベースエンジンと組み合わせて利用できます。

<div id="unity-catalog">
  ## Unity Catalog
</div>

Unity Catalog は、Delta Lake テーブルと Iceberg テーブルの両方で使用できます。`iceberg` または `deltaLake` テーブルエンジンと組み合わせて使用することも、[DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) データベースエンジンと組み合わせて使用することもできます。

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

Microsoft OneLake は、Delta Lake テーブルと Iceberg テーブルの両方で使用できます。[DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog) データベースエンジンでも使用できます。