---
description: 'Documentación sobre lagos de datos'
sidebar_label: 'Lagos de datos'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'Lagos de datos'
doc_type: 'reference'
---

En esta sección, veremos la compatibilidad de ClickHouse con los lagos de datos.
ClickHouse es compatible con muchos de los formatos de tabla y catálogos de datos más populares, incluidos Iceberg, Delta Lake, Hudi, AWS Glue, catálogo REST, Unity Catalog y Microsoft OneLake.

<div id="open-table-formats">
  # Formatos de tabla abiertos
</div>

<div id="iceberg">
  ## Iceberg
</div>

Consulte [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg), que permite leer desde Amazon S3 y servicios compatibles con S3, HDFS, Azure y sistemas de archivos locales. [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) es la variante distribuida de la función `iceberg`.

<div id="delta-lake">
  ## Delta Lake
</div>

Consulte [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake), que permite leer desde Amazon S3, servicios compatibles con S3, Azure y sistemas de archivos locales. [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) es la variante distribuida de la función `deltaLake`.

<div id="hudi">
  ## Hudi
</div>

Consulte [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi), que permite leer desde Amazon S3 y servicios compatibles con S3. [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) es la variante distribuida de la función `hudi`.

<div id="data-catalogs">
  # Catálogos de datos
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

AWS Glue Data Catalog se puede utilizar con tablas Iceberg. Puede usarlo con el motor de tabla `iceberg` o con el motor de base de datos [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="iceberg-rest-catalog">
  ## Catálogo REST de Iceberg
</div>

El catálogo REST de Iceberg se puede usar con tablas Iceberg. Puede usarse con el motor de tabla `iceberg` o con el motor de base de datos [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="unity-catalog">
  ## Unity Catalog
</div>

Unity Catalog puede usarse tanto con tablas Delta Lake como con tablas Iceberg. Puede utilizarlo con los motores de tabla `iceberg` o `deltaLake`, o con el motor de base de datos [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

Microsoft OneLake puede utilizarse tanto con tablas Delta Lake como con tablas Iceberg. Puede usarse con el motor de base de datos [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).