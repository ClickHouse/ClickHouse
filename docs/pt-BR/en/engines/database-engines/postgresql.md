---
description: 'Permite conectar-se a bancos de dados em um servidor PostgreSQL remoto.'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

Permite conectar-se a bancos de dados em um servidor [PostgreSQL](https://www.postgresql.org) remoto. Oferece suporte a operações de leitura e gravação (consultas `SELECT` e `INSERT`) para trocar dados entre o ClickHouse e o PostgreSQL.

Fornece acesso em tempo real à lista de tabelas e à estrutura das tabelas no PostgreSQL remoto por meio das consultas `SHOW TABLES` e `DESCRIBE TABLE`.

Oferece suporte a modificações na estrutura das tabelas (`ALTER TABLE ... ADD|DROP COLUMN`). Se o parâmetro `use_table_cache` (consulte os Parâmetros do motor abaixo) estiver definido como `1`, a estrutura da tabela ficará em cache e não será verificada em busca de modificações, mas poderá ser atualizada com as consultas `DETACH` e `ATTACH`.

<div id="creating-a-database">
  ## Criando um banco de dados
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**Parâmetros do motor**

* `host:port` — Endereço do servidor PostgreSQL.
* `database` — Nome do banco de dados remoto.
* `user` — Usuário do PostgreSQL.
* `password` — Senha do usuário.
* `schema` — Schema do PostgreSQL.
* `use_table_cache` —  Indica se a estrutura da tabela do banco de dados é mantida em cache ou não. Opcional. Valor padrão: `0`.

<div id="data_types-support">
  ## Suporte a tipos de dados
</div>

| PostgreSQL       | ClickHouse                                                                    |
| ---------------- | ----------------------------------------------------------------------------- |
| DATE             | [Date](../../sql-reference/data-types/date.md)                                |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)                        |
| REAL             | [Float32](../../sql-reference/data-types/float.md)                            |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)                            |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (consulte a nota abaixo) |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)                           |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)                           |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)                           |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)                          |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)                          |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)                            |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))                 |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)                              |

:::note
O `numeric(p, 0)` do PostgreSQL com precisão `p` maior que 76 (o máximo suportado por `Decimal256`) — por exemplo, `numeric(78, 0)`, comumente usado para armazenar inteiros de 256 bits — é mapeado para [`Int256`](../../sql-reference/data-types/int-uint.md) em vez de `Decimal`. Valores que não se encaixam no intervalo de `Int256` são rejeitados com erro.
:::

<div id="examples-of-use">
  ## Exemplos de uso
</div>

Banco de dados no ClickHouse que troca dados com o servidor PostgreSQL:

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

Lendo dados da tabela do PostgreSQL:

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

Gravação de dados na tabela PostgreSQL:

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

Suponha que a estrutura da tabela tenha sido modificada no PostgreSQL:

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

Como o parâmetro `use_table_cache` foi definido como `1` quando o banco de dados foi criado, a estrutura da tabela no ClickHouse foi armazenada em cache e, por isso, não foi modificada:

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

Após desanexar a tabela e anexá-la novamente, a estrutura foi atualizada:

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## Conteúdo relacionado
</div>

* Blog: [ClickHouse e PostgreSQL — um casamento perfeito no paraíso dos dados — parte 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* Blog: [ClickHouse e PostgreSQL — um casamento perfeito no paraíso dos dados — parte 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)