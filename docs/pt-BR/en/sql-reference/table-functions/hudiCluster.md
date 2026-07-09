---
description: 'Uma extensão da função de tabela hudi. Permite processar arquivos de
  tabelas Apache Hudi no Amazon S3 em paralelo com vários nós em um cluster especificado.'
sidebar_label: 'hudiCluster'
sidebar_position: 86
slug: /sql-reference/table-functions/hudiCluster
title: 'Função de tabela hudiCluster'
doc_type: 'reference'
---

Esta é uma extensão da função de tabela [hudi](/pt-BR/sql-reference/table-functions/hudi.md).

Permite processar arquivos de tabelas Apache [Hudi](https://hudi.apache.org/) no Amazon S3 em paralelo com vários nós em um cluster especificado. No iniciador, ela cria uma conexão com todos os nós do cluster e distribui cada arquivo dinamicamente. No nó worker, ela consulta o iniciador para saber qual é a próxima tarefa a ser processada e a processa. Isso se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
hudiCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                                    | Descrição                                                                                                                                                                                                                                                                                                                                                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                               | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                                                                                                                                                                                                                                                                                    |
| `url`                                        | URL do bucket com o caminho para uma tabela Hudi existente no S3.                                                                                                                                                                                                                                                                                                                                                          |
| `aws_access_key_id`, `aws_secret_access_key` | Credenciais de longo prazo para o usuário da conta [AWS](https://aws.amazon.com/). Você pode usá-las para autenticar suas requisições. Esses parâmetros são opcionais. Se as credenciais não forem especificadas, serão usadas as credenciais da configuração do ClickHouse. Para mais informações, consulte [Using S3 for Data Storage](/pt-BR/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3). |
| `format`                                     | O [formato](/pt-BR/interfaces/formats) do arquivo.                                                                                                                                                                                                                                                                                                                                                                               |
| `structure`                                  | Estrutura da tabela. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                               |
| `compression`                                | O parâmetro é opcional. Valores compatíveis: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. Por padrão, a compressão será detectada automaticamente pela extensão do arquivo.                                                                                                                                                                                                                                      |
| `extra_credentials`                          | O parâmetro é opcional. Usado para passar um `role_arn` para acesso baseado em função no ClickHouse Cloud. Consulte [Secure S3](/pt-BR/cloud/data-sources/secure-s3) para ver as etapas de configuração.                                                                                                                                                                                                                         |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para ler dados, a partir do cluster, da tabela Hudi especificada no S3.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor é `NULL`.
* `_time` — Data e hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se esse horário for desconhecido, o valor é `NULL`.
* `_etag` — ETag do arquivo. Tipo: `LowCardinality(String)`. Se o ETag for desconhecido, o valor é `NULL`.

<div id="related">
  ## Relacionados
</div>

* [motor Hudi](/pt-BR/engines/table-engines/integrations/hudi.md)
* [função de tabela Hudi](/pt-BR/sql-reference/table-functions/hudi.md)