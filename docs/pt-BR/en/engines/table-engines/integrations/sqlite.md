---
description: 'O motor permite importar e exportar dados para o SQLite e oferece suporte a consultas
  em tabelas SQLite diretamente do ClickHouse.'
sidebar_label: 'SQLite'
sidebar_position: 185
slug: /engines/table-engines/integrations/sqlite
title: 'Motor de tabela SQLite'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="sqlite-table-engine">
  # Motor de tabela SQLite
</div>

<CloudNotSupportedBadge />

Esse motor permite importar e exportar dados para o SQLite e consultar tabelas do SQLite diretamente do ClickHouse.

<div id="creating-a-table">
  ## Criando uma tabela
</div>

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Parâmetros do motor**

* `db_path` — Caminho para o arquivo SQLite de um banco de dados.
* `table` — Nome de uma tabela no banco de dados SQLite ou uma consulta passada ao SQLite sem alterações (consulte [Passando uma consulta em vez de um nome de tabela](#passing-a-query)).

<div id="passing-a-query">
  ## Passando uma consulta em vez de um nome de tabela
</div>

Em vez de um nome de tabela, o argumento `table` pode ser uma consulta `SELECT` enviada ao SQLite tal como está. A estrutura da tabela é inferida com base no resultado da consulta. A consulta pode ser escrita tanto como uma subconsulta quanto encapsulada na função `query`:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Essa tabela é somente leitura: `INSERT` nela não é permitido. A mesma sintaxe é compatível com a função de tabela [`sqlite`](/pt-BR/sql-reference/table-functions/sqlite).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e serializada novamente antes de ser enviada ao SQLite. Portanto, ela deve ser um ClickHouse SQL válido. Para passar uma sintaxe específica do SQLite que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao SQLite literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta ClickHouse ao redor **não** é propagado para a consulta enviada — ele é aplicado no ClickHouse depois que o resultado completo da consulta é obtido. Para restringir os dados lidos do SQLite, coloque o filtro dentro da consulta enviada. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não pode ser propagado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

<div id="data-types-support">
  ## Suporte a tipos de dados
</div>

Quando você especifica explicitamente os tipos de colunas do ClickHouse na definição da tabela, os seguintes tipos do ClickHouse podem ser interpretados a partir de colunas TEXT do SQLite:

* [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
* [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
* [UUID](../../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
* [FixedString](../../../sql-reference/data-types/fixedstring.md)
* Todos os tipos inteiros ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../../sql-reference/data-types/float.md)

Consulte o [motor de banco de dados SQLite](../../../engines/database-engines/sqlite.md#data_types-support) para ver o mapeamento de tipos padrão.

<div id="usage-example">
  ## Exemplo de uso
</div>

Mostra uma consulta que cria a tabela no SQLite:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Retorna os dados da tabela:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**Veja também**

* motor [SQLite](../../../engines/database-engines/sqlite.md)
* função de tabela [sqlite](../../../sql-reference/table-functions/sqlite.md)