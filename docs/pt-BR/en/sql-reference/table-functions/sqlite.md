---
description: 'Permite realizar consultas em dados armazenados em um banco de dados SQLite.'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

Permite realizar consultas em dados armazenados em um banco de dados [SQLite](../../engines/database-engines/sqlite.md).

<div id="syntax">
  ## Sintaxe
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## Argumentos
</div>

* `db_path` — Caminho para um arquivo com um banco de dados SQLite. [String](../../sql-reference/data-types/string.md).
* `table_name` — Nome de uma tabela no banco de dados SQLite ou de uma consulta passada ao SQLite sem alterações (consulte [Passar uma consulta em vez de um nome de tabela](#passing-a-query)). [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## Valor retornado
</div>

* Um objeto de tabela com as mesmas colunas da tabela `SQLite` original.

<div id="passing-a-query">
  ## Usar uma consulta em vez do nome de uma tabela
</div>

Em vez de um nome de tabela, o segundo argumento pode ser uma consulta `SELECT` passada ao SQLite como está. A estrutura da tabela resultante é inferida a partir do resultado da consulta. A consulta pode ser escrita como uma subconsulta ou encapsulada na função `query`:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Essa tabela é somente leitura: `INSERT` nela não é permitido. A mesma sintaxe é compatível com o motor de tabela [`SQLite`](/pt-BR/engines/table-engines/integrations/sqlite).

:::note
A forma de subconsulta `(SELECT ...)` é analisada pelo ClickHouse e reserializada antes de ser enviada ao SQLite. Portanto, ela deve ser um SQL válido do ClickHouse. Para passar uma sintaxe específica do SQLite que o ClickHouse não analisa, use a forma `query('...')`, cujo texto é enviado ao SQLite literalmente.

Qualquer `WHERE`, `LIMIT`, agregação etc. externo da consulta ClickHouse correspondente **não** é propagado para a consulta enviada — ele é aplicado no ClickHouse depois que todo o resultado da consulta é buscado. Para restringir os dados lidos do SQLite, coloque o filtro dentro da consulta enviada. Com [`external_table_strict_query = 1`](/pt-BR/operations/settings/settings#external_table_strict_query), um filtro externo que não pode ser propagado é rejeitado com uma exceção, em vez de ser aplicado localmente.
:::

<div id="example">
  ## Exemplo
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## Relacionado
</div>

* motor de tabela [SQLite](../../engines/table-engines/integrations/sqlite.md)
* [Mecanismo de banco de dados SQLite](../../engines/database-engines/sqlite.md) — seção de suporte a tipos de dados