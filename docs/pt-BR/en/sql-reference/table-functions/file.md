---
description: 'Um engine de tabela que fornece uma interface semelhante a uma tabela para
  fazer SELECT de arquivos e INSERT em arquivos, semelhante à função de tabela s3.
  Use `file` ao trabalhar com arquivos locais e `s3` ao trabalhar com buckets em
  armazenamento de objetos, como S3, GCS ou MinIO.'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # Função de tabela file
</div>

Uma engine de tabela que fornece uma interface semelhante a uma tabela para fazer `SELECT` de arquivos e `INSERT` em arquivos, semelhante à função de tabela [s3](/pt-BR/sql-reference/table-functions/s3.md). Use `file` ao trabalhar com arquivos locais e `s3` ao trabalhar com buckets em armazenamento de objetos, como S3, GCS ou MinIO.

A função `file` pode ser usada em queries `SELECT` e `INSERT` para ler ou gravar arquivos.

<div id="syntax">
  ## Sintaxe
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

Em consultas `SELECT`, `path` também pode ser uma expressão que retorna um `Array(String)`:

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## Argumentos
</div>

| Parâmetro         | Descrição                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | O caminho relativo do arquivo em [user&#95;files&#95;path](/pt-BR/operations/server-configuration-parameters/settings.md#user_files_path), ou um `Array(String)` de caminhos em consultas `SELECT`. No modo somente leitura, oferece suporte aos seguintes [globs](#globs-in-path): `*`, `?`, `{abc,def}` (em que `'abc'` e `'def'` são strings) e `{N..M}` (em que `N` e `M` são números). |
| `path_to_archive` | O caminho relativo para um arquivo zip/tar/7z. Oferece suporte aos mesmos globs de `path`.                                                                                                                                                                                                                                                                                            |
| `format`          | O [formato](/pt-BR/interfaces/formats) do arquivo.                                                                                                                                                                                                                                                                                                                                          |
| `structure`       | Estrutura da tabela. Formato: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                          |
| `compression`     | O tipo de compressão existente quando usado em uma consulta `SELECT`, ou o tipo de compressão desejado quando usado em uma consulta `INSERT`. Os tipos de compressão compatíveis são `gz`, `br`, `xz`, `zst`, `lz4` e `bz2`.                                                                                                                                                          |

:::tip
Quando o argumento `structure` é omitido, o ClickHouse infere o esquema a partir do próprio formato.
Formatos diferentes geram nomes e tipos de coluna padrão diferentes.
Para ver o esquema de um formato específico, use [`DESC`](/pt-BR/sql-reference/statements/describe-table) com a função de tabela [`format`](/pt-BR/sql-reference/table-functions/format).

Por exemplo:

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## Valor retornado
</div>

Uma tabela para leitura ou gravação de dados em um arquivo.

<div id="examples-for-writing-to-a-file">
  ## Exemplos de gravação em um arquivo
</div>

<div id="write-to-a-tsv-file">
  ### Gravar em um arquivo TSV
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

Como resultado, os dados são gravados no arquivo `test.tsv`:

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### Escrita particionada em vários arquivos TSV
</div>

Se você especificar uma expressão `PARTITION BY` ao inserir dados em uma função de tabela do tipo `file`, será criado um arquivo separado para cada partição. Dividir os dados em arquivos separados ajuda a melhorar o desempenho das operações de leitura.

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

Como resultado, os dados são gravados em três arquivos: `test_1.tsv`, `test_2.tsv` e `test_3.tsv`.

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## Exemplos de leitura a partir de um arquivo
</div>

<div id="select-from-a-csv-file">
  ### SELECT em um arquivo CSV
</div>

Primeiro, defina `user_files_path` na configuração do servidor e prepare um arquivo `test.csv`:

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

Em seguida, leia os dados de `test.csv` para uma tabela e selecione as duas primeiras linhas:

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### Inserindo dados de um arquivo em uma tabela
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Lendo dados de `table.csv`, que está em `archive1.zip` ou/e `archive2.zip`:

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## Globs no caminho
</div>

Os caminhos podem usar globs. Os arquivos devem corresponder ao padrão do caminho completo, não apenas ao sufixo ou ao prefixo. Há uma exceção: se o caminho se referir a um diretório existente
e não usar globs, um `*` será adicionado implicitamente ao caminho para que
todos os arquivos no diretório sejam selecionados.

* `*` — Representa uma quantidade arbitrária de caracteres, exceto `/`, incluindo a string vazia.
* `?` — Representa um único caractere arbitrário.
* `{some_string,another_string,yet_another_one}` — Substitui qualquer uma das strings `'some_string', 'another_string', 'yet_another_one'`. As strings podem conter o símbolo `/`.
* `{N..M}` — Representa qualquer número `>= N` e `<= M`.
* `**` - Representa recursivamente todos os arquivos dentro de uma pasta.

As construções com `{}` são semelhantes às funções de tabela [remote](remote.md) e [hdfs](hdfs.md).

<div id="examples">
  ## Exemplos
</div>

**Exemplo**

Suponha que haja estes arquivos nos seguintes caminhos relativos:

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

Consulte o número total de linhas em todos os arquivos:

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

Uma expressão de caminho alternativa que produz o mesmo resultado:

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

Consulte o total de linhas em `some_dir` usando `*` implícito:

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
Se a listagem de arquivos contiver intervalos numéricos com zeros à esquerda, use a construção com chaves para cada dígito separadamente ou `?`.
:::

**Exemplo**

Consulte o número total de linhas nos arquivos chamados `file000`, `file001`, ... , `file999`:

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**Exemplo**

Consulte o número total de linhas em todos os arquivos no diretório `big_dir/`, recursivamente:

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**Exemplo**

Consulte recursivamente o número total de linhas de todos os arquivos `file002` em qualquer pasta do diretório `big_dir/`:

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho do arquivo for desconhecido, o valor será `NULL`.
* `_time` — Hora da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se a hora for desconhecida, o valor será `NULL`.

<div id="hive-style-partitioning">
  ## configuração `use_hive_partitioning`
</div>

Quando a configuração `use_hive_partitioning` é definida como 1, o ClickHouse detecta o particionamento no estilo Hive no caminho (`/name=value/`) e permite usar colunas de partição como colunas virtuais na consulta. Essas colunas virtuais terão os mesmos nomes do caminho particionado.

**Exemplo**

Usar uma coluna virtual criada com particionamento no estilo Hive

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## Configurações
</div>

| Configuração                                                                                                                            | Descrição                                                                                                                                                                                                 |
| --------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/pt-BR/operations/settings/settings#engine_file_empty_if_not_exists)                    | permite selecionar dados vazios de um arquivo inexistente. Desativado por padrão.                                                                                                                         |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/pt-BR/operations/settings/settings#engine_file_truncate_on_insert)                          | permite truncar o arquivo antes de inserir dados nele. Desativado por padrão.                                                                                                                             |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/pt-BR/operations/settings/settings.md#engine_file_allow_create_multiple_files) | permite criar um novo arquivo a cada inserção se o format tiver sufixo. Desativado por padrão.                                                                                                            |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/pt-BR/operations/settings/settings.md#engine_file_skip_empty_files)                           | permite ignorar arquivos vazios durante a leitura. Desativado por padrão.                                                                                                                                 |
| [storage&#95;file&#95;read&#95;method](/pt-BR/operations/settings/settings#engine_file_empty_if_not_exists)                                   | método de leitura dos dados do arquivo de armazenamento; um dos seguintes: read, pread, mmap (somente para clickhouse-local). Valor padrão: `pread` para clickhouse-server, `mmap` para clickhouse-local. |

<div id="related">
  ## Relacionados
</div>

* [Colunas virtuais](/pt-BR/engines/table-engines/index.md#table_engines-virtual_columns)
* [Renomear arquivos após o processamento](/pt-BR/operations/settings/settings.md#rename_files_after_processing)