---
description: 'Uma extensão da função de tabela s3, que permite processar arquivos
  do Amazon S3 e do Google Cloud Storage em paralelo usando muitos nós em um
  cluster especificado.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

Esta é uma extensão da função de tabela [s3](/pt-BR/sql-reference/table-functions/s3.md).

Permite processar arquivos do [Amazon S3](https://aws.amazon.com/s3/) e do Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) em paralelo usando muitos nós em um cluster especificado. No iniciador, ela cria uma conexão com todos os nós do cluster, expande os asteriscos no caminho do arquivo S3 e distribui cada arquivo dinamicamente. No nó worker, ela consulta o iniciador sobre a próxima tarefa a ser processada e a processa. Isso se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento                               | Descrição                                                                                                                                                                                                                                                                                                                  |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                          | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                                                                                                                                                                                    |
| `url`                                   | caminho para um arquivo ou um conjunto de arquivos. Suporta os seguintes wildcards no modo readonly: `*`, `**`, `?`, `{'abc','def'}` e `{N..M}`, em que `N`, `M` — números, `abc`, `def` — strings. Para mais informações, consulte [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                                | Se esta palavra-chave for fornecida no lugar das credenciais, nenhuma das requisições será assinada.                                                                                                                                                                                                                       |
| `access_key_id` and `secret_access_key` | Chaves que especificam as credenciais a serem usadas com o endpoint informado. Opcional.                                                                                                                                                                                                                                   |
| `session_token`                         | Token de sessão a ser usado com as chaves fornecidas. Opcional ao informar chaves.                                                                                                                                                                                                                                         |
| `format`                                | O [formato](/pt-BR/sql-reference/formats) do arquivo.                                                                                                                                                                                                                                                                            |
| `structure`                             | Estrutura da tabela. Formato `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                |
| `compression_method`                    | O parâmetro é opcional. Valores compatíveis: `none`, `gzip` ou `gz`, `brotli` ou `br`, `xz` ou `LZMA`, `zstd` ou `zst`. Por padrão, o método de compressão será detectado automaticamente pela extensão do arquivo.                                                                                                        |
| `headers`                               | O parâmetro é opcional. Permite passar cabeçalhos na requisição ao S3. Informe no formato `headers(key=value)`, por exemplo `headers('x-amz-request-payer' = 'requester')`. Consulte [here](/pt-BR/sql-reference/table-functions/s3#accessing-requester-pays-buckets) para ver um exemplo de uso.                                |
| `extra_credentials`                     | Opcional. `roleARN` pode ser passado por meio deste parâmetro. Consulte [here](/pt-BR/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role) para ver um exemplo.                                                                                                                                    |

Os argumentos também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md). Nesse caso, `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` funcionam da mesma forma, e alguns parâmetros extras são compatíveis:

| Argumento                     | Descrição                                                                                                                                                                                                                                 |
| ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `filename`                    | anexado à URL, se especificado.                                                                                                                                                                                                           |
| `use_environment_credentials` | habilitado por padrão, permite passar parâmetros extras usando as variáveis de ambiente `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | desabilitado por padrão.                                                                                                                                                                                                                  |
| `expiration_window_seconds`   | o valor padrão é 120.                                                                                                                                                                                                                     |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para leitura ou gravação de dados no arquivo especificado.

<div id="examples">
  ## Exemplos
</div>

Selecione os dados de todos os arquivos das pastas `/root/data/clickhouse` e `/root/data/database/`, usando todos os nós do cluster `cluster_simple`:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Conte o número total de linhas em todos os arquivos do cluster `cluster_simple`:

:::tip
Se a sua listagem de arquivos contiver intervalos numéricos com zeros à esquerda, use a construção com chaves para cada dígito separadamente ou use `?`.
:::

Para uso em produção, recomenda-se usar [coleções nomeadas](/pt-BR/operations/named-collections.md). Veja o exemplo:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## Acessando buckets privados e públicos
</div>

Os usuários podem usar as mesmas abordagens descritas para a função s3 [aqui](/pt-BR/sql-reference/table-functions/s3#accessing-public-buckets).

<div id="optimizing-performance">
  ## Otimizando o desempenho
</div>

Para saber mais sobre como otimizar o desempenho da função s3, consulte [nosso guia detalhado](/pt-BR/integrations/s3/performance).

<div id="related">
  ## Relacionados
</div>

* [motor S3](../../engines/table-engines/integrations/s3.md)
* [função de tabela S3](../../sql-reference/table-functions/s3.md)