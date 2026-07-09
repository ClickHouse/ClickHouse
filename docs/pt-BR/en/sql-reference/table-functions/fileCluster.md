---
description: 'Permite o processamento simultâneo de arquivos que correspondem a um caminho
  especificado em vários nós de um cluster. O initiator estabelece connections
  com nós worker, expande globs no caminho e delega tarefas de leitura
  de arquivos aos nós worker. Cada nó worker consulta o initiator para obter o
  próximo arquivo a ser processado, repetindo o processo até que todas as tarefas sejam concluídas
  (todos os arquivos sejam lidos).'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

Permite o processamento simultâneo de arquivos que correspondem a um caminho especificado em vários nós de um cluster. O initiator estabelece connections com nós worker, expande globs no caminho e delega tarefas de leitura de arquivos aos nós worker. Cada nó worker consulta o initiator para obter o próximo arquivo a ser processado, repetindo o processo até que todas as tarefas sejam concluídas (todos os arquivos sejam lidos).

:::note
Esta função só funcionará *corretamente* se o conjunto de arquivos que corresponde ao caminho especificado inicialmente for idêntico em todos os nós, e se o conteúdo desses arquivos for consistente entre os diferentes nós.
Se esses arquivos forem diferentes entre os nós, o valor de retorno não poderá ser determinado previamente e dependerá da ordem em que os nós worker solicitarem tarefas ao initiator.
:::

<div id="syntax">
  ## Sintaxe
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento            | Descrição                                                                                                                                                                                             |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | Nome de um cluster usado para montar um conjunto de endereços e parâmetros de conexão para servidores remotos e locais.                                                                               |
| `path`               | O caminho relativo para o arquivo em [user&#95;files&#95;path](/pt-BR/operations/server-configuration-parameters/settings.md#user_files_path). O caminho do arquivo também suporta [globs](#globs-in-path). |
| `format`             | [Formato](/pt-BR/sql-reference/formats) dos arquivos. Tipo: [String](../../sql-reference/data-types/string.md).                                                                                             |
| `structure`          | Estrutura da tabela no formato `'UserID UInt64, Name String'`. Determina os nomes e tipos das colunas. Tipo: [String](../../sql-reference/data-types/string.md).                                      |
| `compression_method` | Método de compressão. Os tipos de compressão compatíveis são `gz`, `br`, `xz`, `zst`, `lz4` e `bz2`.                                                                                                  |

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela com o formato e a estrutura especificados e com dados de arquivos que correspondem ao caminho especificado.

**Exemplo**

Dado um cluster chamado `my_cluster` e o seguinte valor da configuração `user_files_path`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

Além disso, considerando que há os arquivos `test1.csv` e `test2.csv` em `user_files_path` de cada nó do cluster, e que o conteúdo deles é idêntico entre os diferentes nós:

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

Por exemplo, é possível criar esses arquivos executando essas duas consultas em cada nó do cluster:

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

Agora, leia o conteúdo dos arquivos `test1.csv` e `test2.csv` por meio da função de tabela `fileCluster`:

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## Globs no caminho
</div>

Todos os padrões compatíveis com a função de tabela [File](../../sql-reference/table-functions/file.md#globs-in-path) também são compatíveis com o FileCluster.

<div id="related">
  ## Relacionados
</div>

* [função de tabela File](../../sql-reference/table-functions/file.md)