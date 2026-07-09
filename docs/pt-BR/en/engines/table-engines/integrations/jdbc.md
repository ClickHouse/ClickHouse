---
description: 'Permite que o ClickHouse se conecte a bancos de dados externos por JDBC.'
sidebar_label: 'JDBC'
sidebar_position: 100
slug: /engines/table-engines/integrations/jdbc
title: 'motor de tabela JDBC'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="jdbc-table-engine">
  # motor de tabela JDBC
</div>

<CloudNotSupportedBadge />

:::note
clickhouse-jdbc-bridge contém código experimental e não tem mais suporte. Ele pode apresentar problemas de confiabilidade e vulnerabilidades de segurança. Use por sua conta e risco.
A ClickHouse recomenda usar as table functions nativas do ClickHouse, que oferecem uma alternativa melhor para cenários de consultas ad hoc (Postgres, MySQL, MongoDB etc.).
:::

Permite que o ClickHouse se conecte a bancos de dados externos via [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Para implementar a conexão JDBC, o ClickHouse usa o programa separado [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge), que deve ser executado como um daemon.

Este engine é compatível com o tipo de dados [Nullable](../../../sql-reference/data-types/nullable.md).

<div id="creating-a-table">
  ## Criação de uma tabela
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(datasource, external_database, external_table)
```

**Parâmetros do engine**

* `datasource` — URI ou nome de um SGBD externo.

  Formato da URI: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
  Exemplo para MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

* `external_database` — Nome de um banco de dados em um SGBD externo ou, alternativamente, um esquema de tabela definido explicitamente (veja os exemplos).

* `external_table` — Nome da tabela em um banco de dados externo ou uma consulta `select`, como `select * from table1 where column1=1`.

* Esses parâmetros também podem ser passados usando [coleções nomeadas](/pt-BR/operations/named-collections.md).

<div id="usage-example">
  ## Exemplo de uso
</div>

Criando uma tabela no servidor MySQL conectando-se diretamente a ele com o cliente de console:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Criando uma tabela no servidor ClickHouse e selecionando dados dessa tabela:

```sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

```sql
SELECT *
FROM jdbc_table
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

```sql
INSERT INTO jdbc_table(`int_id`, `float`)
SELECT toInt32(number), toFloat32(number * 1.0)
FROM system.numbers
```

<div id="see-also">
  ## Veja também
</div>

* [função de tabela JDBC](../../../sql-reference/table-functions/jdbc.md).