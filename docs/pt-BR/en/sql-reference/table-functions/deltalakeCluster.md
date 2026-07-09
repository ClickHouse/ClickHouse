---
description: 'Esta é uma extensão da função de tabela deltaLake.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

Esta é uma extensão da função de tabela [deltaLake](/pt-BR/sql-reference/table-functions/deltalake.md).

Permite processar, em paralelo, arquivos de tabelas [Delta Lake](https://github.com/delta-io/delta) no Amazon S3 a partir de vários nós em um cluster especificado. No initiator, ela cria uma conexão com todos os nós do cluster e distribui dinamicamente cada arquivo. No nó worker, ela consulta o initiator sobre a próxima tarefa a ser processada e a executa. Esse processo se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` é um alias para `deltaLakeCluster`; ambos são para S3.

<div id="arguments">
  ## Argumentos
</div>

* `cluster_name` — Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.
* A descrição de todos os outros argumentos coincide com a descrição dos argumentos na [função de tabela](/pt-BR/sql-reference/table-functions/deltalake.md) [deltaLake] equivalente.
* Um parâmetro opcional `extra_credentials` pode ser usado para fornecer um `role_arn` para acesso baseado em função no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para ler dados do cluster na tabela Delta Lake especificada no S3.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor é `NULL`.
* `_time` — Hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor é `NULL`.
* `_etag` — O etag do arquivo. Tipo: `LowCardinality(String)`. Se o etag for desconhecido, o valor é `NULL`.

<div id="related">
  ## Relacionados
</div>

* [motor DeltaLake](/pt-BR/engines/table-engines/integrations/deltalake.md)
* [função de tabela DeltaLake](/pt-BR/sql-reference/table-functions/deltalake.md)