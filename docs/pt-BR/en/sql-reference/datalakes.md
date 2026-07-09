---
description: 'Documentação sobre lagos de dados'
sidebar_label: 'Lagos de dados'
sidebar_position: 2
slug: /sql-reference/datalakes
title: 'Lagos de dados'
doc_type: 'reference'
---

Nesta seção, veremos o suporte do ClickHouse a lagos de dados.
O ClickHouse oferece suporte a muitos dos formatos de tabela e catálogos de dados mais populares, incluindo Iceberg, Delta Lake, Hudi, AWS Glue, REST Catalog, Unity Catalog e Microsoft OneLake.

<div id="open-table-formats">
  # Formatos de tabela abertos
</div>

<div id="iceberg">
  ## Iceberg
</div>

Veja [iceberg](https://clickhouse.com/docs/sql-reference/table-functions/iceberg), que oferece suporte à leitura no Amazon S3 e em serviços compatíveis com S3, HDFS, Azure e sistemas de arquivos locais. [icebergCluster](https://clickhouse.com/docs/sql-reference/table-functions/icebergCluster) é a variante distribuída da função `iceberg`.

<div id="delta-lake">
  ## Delta Lake
</div>

Consulte [deltaLake](https://clickhouse.com/docs/sql-reference/table-functions/deltalake), que oferece suporte à leitura do Amazon S3, de serviços compatíveis com S3, do Azure e de sistemas de arquivos locais. [deltaLakeCluster](https://clickhouse.com/docs/sql-reference/table-functions/deltalakeCluster) é a variante distribuída da função `deltaLake`.

<div id="hudi">
  ## Hudi
</div>

Consulte [hudi](https://clickhouse.com/docs/sql-reference/table-functions/hudi), que oferece suporte à leitura no Amazon S3 e em serviços compatíveis com S3. [hudiCluster](https://clickhouse.com/docs/sql-reference/table-functions/hudiCluster) é a variante distribuída da função `hudi`.

<div id="data-catalogs">
  # Catálogos de dados
</div>

<div id="aws-glue">
  ## AWS Glue
</div>

O AWS Glue Data Catalog pode ser usado com tabelas Iceberg. Você pode usá-lo com o mecanismo de tabela `iceberg` ou com o motor de banco de dados [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="iceberg-rest-catalog">
  ## Iceberg REST Catalog
</div>

O Iceberg REST Catalog pode ser usado com tabelas Iceberg. Você pode usá-lo com o mecanismo de tabela `iceberg` ou com o motor de banco de dados [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="unity-catalog">
  ## Unity Catalog
</div>

O Unity Catalog pode ser usado com tabelas Delta Lake e Iceberg. Você pode usá-lo com os motores de tabela `iceberg` ou `deltaLake`, ou com o motor de banco de dados [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).

<div id="microsoft-onelake">
  ## Microsoft OneLake
</div>

O Microsoft OneLake pode ser usado com tabelas Delta Lake e Iceberg. Você pode usá-lo com o motor de banco de dados [DataLakeCatalog](https://clickhouse.com/docs/engines/database-engines/datalakecatalog).