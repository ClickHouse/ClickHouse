---
description: 'Uma extensão da função de tabela paimon que permite processar arquivos
  do Apache Paimon em paralelo a partir de vários nós em um cluster especificado.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'referência'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # Função de tabela paimonCluster
</div>

<ExperimentalBadge />

Esta é uma extensão da função de tabela [paimon](/pt-BR/sql-reference/table-functions/paimon.md).

Permite processar arquivos do Apache [Paimon](https://paimon.apache.org/) em paralelo em vários nós de um cluster especificado. No iniciador, ela cria uma conexão com todos os nós do cluster e distribui dinamicamente cada arquivo. No nó worker, ela solicita ao iniciador a próxima tarefa a ser processada e a executa. Esse processo se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## Argumentos
</div>

* `cluster_name` — Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.
* A descrição de todos os demais argumentos é a mesma da função de tabela [paimon](/pt-BR/sql-reference/table-functions/paimon.md) equivalente.
* Um parâmetro opcional `extra_credentials` pode ser usado para passar um `role_arn` para acesso baseado em funções no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.

**Valor retornado**

Uma tabela com a estrutura especificada para ler dados do cluster na tabela Paimon especificada.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor é `NULL`.
* `_time` — Data e hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a data e hora forem desconhecidas, o valor é `NULL`.
* `_etag` — O etag do arquivo. Tipo: `LowCardinality(String)`. Se o etag for desconhecido, o valor é `NULL`.

**Veja também**

* [função de tabela Paimon](/pt-BR/sql-reference/table-functions/paimon.md)