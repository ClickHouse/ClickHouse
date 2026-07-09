---
description: 'Permite conectar-se ao banco de dados SQLite e executar consultas `INSERT` e `SELECT`
  para trocar dados entre ClickHouse e SQLite.'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

Permite conectar-se ao banco de dados [SQLite](https://www.sqlite.org/index.html) e executar consultas `INSERT` e `SELECT` para trocar dados entre ClickHouse e SQLite.

<div id="creating-a-database">
  ## Criar um banco de dados
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**Parâmetros do mecanismo**

* `db_path` — Caminho para um arquivo que contém o banco de dados SQLite.

<div id="data_types-support">
  ## Suporte a tipos de dados
</div>

A tabela abaixo mostra o mapeamento de tipos padrão quando o ClickHouse infere automaticamente o schema a partir do SQLite:

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

Quando você define explicitamente uma tabela com tipos específicos do ClickHouse usando o [SQLite table engine](../../engines/table-engines/integrations/sqlite.md), os seguintes tipos do ClickHouse podem ser interpretados a partir de colunas TEXT do SQLite:

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* Todos os tipos inteiros ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md))
* [Float32, Float64](../../sql-reference/data-types/float.md)

O SQLite tem tipagem dinâmica, e suas funções de acesso a tipos fazem coerção automática de tipos. Por exemplo, ler uma coluna TEXT como inteiro retornará 0 se o texto não puder ser interpretado como um número. Isso significa que, se uma tabela do ClickHouse for definida com um tipo diferente do tipo da coluna SQLite subjacente, os valores poderão sofrer coerção silenciosamente em vez de gerar um erro.

<div id="specifics-and-recommendations">
  ## Especificidades e recomendações
</div>

O SQLite armazena todo o banco de dados (definições, tabelas, índices e os próprios dados) como um único arquivo multiplataforma em uma máquina hospedeira. Durante a gravação, o SQLite bloqueia todo o arquivo do banco de dados; portanto, as operações de gravação são executadas sequencialmente. As operações de leitura podem ser executadas em paralelo.
O SQLite não requer gerenciamento de serviços (como scripts de inicialização) nem controle de acesso baseado em `GRANT` e senhas. O controle de acesso é feito por meio das permissões do sistema de arquivos atribuídas ao próprio arquivo do banco de dados.

<div id="usage-example">
  ## Exemplo de uso
</div>

Banco de dados no ClickHouse, conectado ao SQLite:

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

Exibe as tabelas:

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

Inserindo dados em uma tabela do SQLite a partir de uma tabela do ClickHouse:

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```