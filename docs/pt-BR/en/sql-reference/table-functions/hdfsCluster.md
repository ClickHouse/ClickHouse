---
description: 'Permite processar arquivos do HDFS em paralelo em vários nós de um
  cluster especificado.'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

Permite processar arquivos do HDFS em paralelo em vários nós de um cluster especificado. No iniciador, ele cria uma conexão com todos os nós do cluster, expande os asteriscos no caminho do arquivo do HDFS e distribui cada arquivo dinamicamente. No nó worker, ele consulta o iniciador para saber qual é a próxima tarefa a ser processada e a executa. Isso se repete até que todas as tarefas sejam concluídas.

<div id="syntax">
  ## Sintaxe
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento      | Descrição                                                                                                                                                                                                                                                                                                                                           |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name` | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                                                                                                                                                                                                             |
| `URI`          | URI de um arquivo ou de um conjunto de arquivos. Suporta os seguintes caracteres curinga no modo somente leitura: `*`, `**`, `?`, `{'abc','def'}` e `{N..M}`, em que `N`, `M` — números, `abc`, `def` — strings. Para mais informações, consulte [Caracteres curinga no caminho](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `format`       | O [formato](/pt-BR/sql-reference/formats) do arquivo.                                                                                                                                                                                                                                                                                                     |
| `structure`    | Estrutura da tabela. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                        |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com a estrutura especificada para leitura dos dados no arquivo especificado.

<div id="examples">
  ## Exemplos
</div>

1. Suponha que há um cluster ClickHouse chamado `cluster_simple` e vários arquivos com os seguintes URIs no HDFS:

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. Consulte a quantidade de linhas nesses arquivos:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. Consulte o número de linhas em todos os arquivos desses dois diretórios:

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
Se a listagem de arquivos contiver intervalos numéricos com zeros à esquerda, use a construção com chaves para cada dígito separadamente ou `?`.
:::

<div id="related">
  ## Relacionados
</div>

* [motor HDFS](../../engines/table-engines/integrations/hdfs.md)
* [função de tabela HDFS](../../sql-reference/table-functions/hdfs.md)