---
description: 'O motor de tabela File armazena os dados em um arquivo em um dos
  formatos de arquivo compatíveis (`TabSeparated`, `Native` etc.).'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'Motor de tabela File'
doc_type: 'reference'
---

O motor de tabela File armazena os dados em um arquivo em um dos [formatos de arquivo](/pt-BR/interfaces/formats#formats-overview) compatíveis (`TabSeparated`, `Native` etc.).

Cenários de uso:

* Exportação de dados do ClickHouse para um arquivo.
* Conversão de dados de um formato para outro.
* Atualização de dados no ClickHouse por meio da edição de um arquivo em disco.

:::note
No momento, este motor não está disponível no ClickHouse Cloud. [Use a função de tabela S3](/pt-BR/sql-reference/table-functions/s3.md).
:::

<div id="usage-in-clickhouse-server">
  ## Uso no servidor ClickHouse
</div>

```sql
File(Format)
```

O parâmetro `Format` especifica um dos formatos de arquivo disponíveis. Para executar
consultas `SELECT`, o formato deve ter suporte para entrada e, para executar
consultas `INSERT`, para saída. Os formatos disponíveis estão listados na seção
[Formatos](/pt-BR/interfaces/formats#formats-overview).

O ClickHouse não permite especificar o caminho no sistema de arquivos para `File`. Ele usará a pasta definida pela configuração [path](../../../operations/server-configuration-parameters/settings.md) na configuração do servidor.

Ao criar uma tabela usando `File(Format)`, é criado um subdiretório vazio nessa pasta. Quando os dados são gravados nessa tabela, eles são salvos no arquivo `data.Format` dentro desse subdiretório.

Você pode criar manualmente essa subpasta e esse arquivo no sistema de arquivos do servidor e, em seguida, fazer [ATTACH](../../../sql-reference/statements/attach.md) deles às informações da tabela com o nome correspondente, para que seja possível consultar os dados desse arquivo.

:::note
Tenha cuidado com essa funcionalidade, porque o ClickHouse não rastreia alterações externas nesses arquivos. O resultado de gravações simultâneas via ClickHouse e fora do ClickHouse é indefinido.
:::

<div id="example">
  ## Exemplo
</div>

**1.** Configure a tabela `file_engine_table`:

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

Por padrão, o ClickHouse criará a pasta `/var/lib/clickhouse/data/default/file_engine_table`.

**2.** Crie manualmente o arquivo `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` com o seguinte conteúdo:

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** Consulte os dados:

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## Uso no ClickHouse-local
</div>

No [clickhouse-local](../../../operations/utilities/clickhouse-local.md), o motor File aceita um caminho de arquivo além de `Format`. Os fluxos padrão de entrada/saída podem ser especificados usando nomes numéricos ou legíveis por pessoas, como `0` ou `stdin`, `1` ou `stdout`. É possível ler e gravar arquivos comprimidos com base em um parâmetro adicional do motor ou na extensão do arquivo (`gz`, `br` ou `xz`).

**Exemplo:**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## Detalhes da implementação
</div>

* Várias consultas `SELECT` podem ser executadas de forma concorrente, mas as consultas `INSERT` precisam esperar umas pelas outras.
* Há suporte à criação de um novo arquivo por meio de uma consulta `INSERT`.
* Se o arquivo já existir, `INSERT` acrescentará novos valores ao final dele.
* Não há suporte para:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * Índices
  * Replicação

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — Opcional. É possível criar arquivos separados particionando os dados com base em uma chave de partição. Na maioria dos casos, você não precisa de uma chave de partição e, mesmo quando ela é necessária, em geral não precisa ser mais granular do que mensal. O particionamento não acelera as consultas (ao contrário da expressão `ORDER BY`). Você nunca deve usar um particionamento excessivamente granular. Não particione seus dados por identificadores ou nomes de clientes (em vez disso, use o identificador ou nome do cliente como a primeira coluna na expressão `ORDER BY`).

Para particionar por mês, use a expressão `toYYYYMM(date_column)`, em que `date_column` é uma coluna com uma data do tipo [Date](/pt-BR/sql-reference/data-types/date.md). Os nomes das partições aqui seguem o formato `"YYYYMM"`.

<div id="virtual-columns">
  ## Colunas virtuais
</div>

* `_path` — Caminho do arquivo. Tipo: `LowCardinality(String)`.
* `_file` — Nome do arquivo. Tipo: `LowCardinality(String)`.
* `_size` — Tamanho do arquivo em bytes. Tipo: `Nullable(UInt64)`. Se o tamanho for desconhecido, o valor é `NULL`.
* `_time` — Horário da última modificação do arquivo. Tipo: `Nullable(DateTime)`. Se o horário for desconhecido, o valor é `NULL`.

<div id="settings">
  ## Configurações
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/pt-BR/operations/settings/settings#engine_file_empty_if_not_exists) - permite selecionar dados vazios de um arquivo que não existe. Desabilitada por padrão.
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/pt-BR/operations/settings/settings#engine_file_truncate_on_insert) - permite truncar o arquivo antes de inserir dados nele. Desabilitada por padrão.
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/pt-BR/operations/settings/settings.md#engine_file_allow_create_multiple_files) - permite criar um novo arquivo a cada insert se o format tiver sufixo. Desabilitada por padrão.
* [engine&#95;file&#95;skip&#95;empty&#95;files](/pt-BR/operations/settings/settings.md#engine_file_skip_empty_files) - permite ignorar arquivos vazios durante a leitura. Desabilitada por padrão.
* [storage&#95;file&#95;read&#95;method](/pt-BR/operations/settings/settings#engine_file_empty_if_not_exists) - método de leitura de dados do arquivo de armazenamento, um destes: `read`, `pread`, `mmap`. O método `mmap` não se aplica ao clickhouse-server (ele se destina ao clickhouse-local). Valor padrão: `pread` para clickhouse-server e `mmap` para clickhouse-local.