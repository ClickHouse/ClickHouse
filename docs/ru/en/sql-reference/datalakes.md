---
description: 'Документация по озёрам данных'
sidebar_label: 'Озёра данных'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'Озёра данных'
doc_type: 'reference'
---

В этом разделе рассматривается поддержка озёр данных в ClickHouse.
ClickHouse поддерживает многие популярные форматы таблиц и каталоги данных, включая Iceberg, Delta Lake, Hudi, AWS Glue, REST-каталог, Unity Catalog и Microsoft OneLake.

<div id="open-table-formats">
  # Открытые табличные форматы
</div>

<div id="iceberg">
  ## Iceberg
</div>

См. [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg), поддерживающую чтение из Amazon S3, S3-совместимых сервисов, HDFS, Azure и локальных файловых систем. [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) — распределённый вариант функции `iceberg`.

<div id="delta-lake">
  ## Delta Lake
</div>

См. [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake), которая поддерживает чтение из Amazon S3, S3-совместимых сервисов, Azure и локальных файловых систем. [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) — распределённый вариант функции `deltaLake`.

<div id="hudi">
  ## Hudi
</div>

См. [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi), которая поддерживает чтение из Amazon S3 и S3-совместимых сервисов. [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) — распределённый вариант функции `hudi`.

<div id="data-catalogs">
  # Каталоги данных
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

Каталог данных AWS Glue можно использовать с таблицами Iceberg. Он поддерживается как в движке таблицы `iceberg`, так и в движке базы данных [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="iceberg-rest-catalog">
  ## REST-каталог Iceberg
</div>

REST-каталог Iceberg можно использовать с таблицами Iceberg. Его можно использовать с движком таблицы `iceberg` или с движком базы данных [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="unity-catalog">
  ## Unity Catalog
</div>

Unity Catalog можно использовать как с таблицами Delta Lake, так и с таблицами Iceberg. Он поддерживается движками таблиц `iceberg` и `deltaLake`, а также движком базы данных [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

Microsoft OneLake можно использовать как с таблицами Delta Lake, так и с таблицами Iceberg. Он поддерживается движком базы данных [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).