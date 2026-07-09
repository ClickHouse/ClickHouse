---
description: 'Documentation des lacs de données'
sidebar_label: 'Lacs de données'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'Lacs de données'
doc_type: 'reference'
---

Dans cette section, nous allons examiner la prise en charge des lacs de données par ClickHouse.
ClickHouse prend en charge bon nombre des formats de table et des catalogues de données les plus répandus, notamment Iceberg, Delta Lake, Hudi, AWS Glue, REST Catalog, Unity Catalog et Microsoft OneLake.

<div id="open-table-formats">
  # Formats de tables ouverts
</div>

<div id="iceberg">
  ## Iceberg
</div>

Voir [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg), qui permet de lire depuis Amazon S3 et les services compatibles S3, HDFS, Azure et les systèmes de fichiers locaux. [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) est la variante distribuée de la fonction `iceberg`.

<div id="delta-lake">
  ## Delta Lake
</div>

Voir [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake), qui permet de lire depuis Amazon S3, les services compatibles S3, Azure et les systèmes de fichiers locaux. [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) est la variante distribuée de la fonction `deltaLake`.

<div id="hudi">
  ## Hudi
</div>

Voir [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi), qui prend en charge la lecture à partir d’Amazon S3 et de services compatibles S3. [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) est la variante distribuée de la fonction `hudi`.

<div id="data-catalogs">
  # Catalogues de données
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

Le service AWS Glue Data Catalog peut être utilisé avec des tables Iceberg. Vous pouvez l’utiliser avec le moteur de table `iceberg` ou avec le moteur de base de données [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="iceberg-rest-catalog">
  ## Iceberg REST Catalog
</div>

Iceberg REST Catalog peut être utilisé avec des tables Iceberg. Vous pouvez l&#39;utiliser avec le moteur de table `iceberg` ou avec le moteur de base de données [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="unity-catalog">
  ## Unity Catalog
</div>

Unity Catalog peut être utilisé avec des tables Delta Lake et Iceberg. Vous pouvez l’utiliser avec les table engines `iceberg` ou `deltaLake`, ou avec le moteur de base de données [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

Microsoft OneLake peut être utilisé avec des tables Delta Lake et Iceberg. Vous pouvez l’utiliser avec le moteur de base de données [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).