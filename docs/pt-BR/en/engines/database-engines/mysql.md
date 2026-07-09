---
description: 'Permite conectar-se a bancos de dados em um servidor MySQL remoto e executar
  consultas `INSERT` e `SELECT` para trocar dados entre ClickHouse e MySQL.'
sidebar_label: 'MySQL'
sidebar_position: 50
slug: /engines/database-engines/mysql
title: 'MySQL'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mysql-database-engine">
  # Mecanismo de banco de dados MySQL
</div>

<CloudNotSupportedBadge />

Permite conectar-se a bancos de dados em um servidor MySQL remoto e executar consultas `INSERT` e `SELECT` para trocar dados entre o ClickHouse e o MySQL.

O mecanismo de banco de dados `MySQL` traduz as consultas para o servidor MySQL, para que você possa executar operações como `SHOW TABLES` ou `SHOW CREATE TABLE`.

Você não pode executar as seguintes consultas:

* `RENAME`
* `CREATE TABLE`
* `ALTER`

<div id="creating-a-database">
  ## Criar um banco de dados
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**Parâmetros do mecanismo**

* `host:port` — Endereço do servidor MySQL.
* `database` — Nome do banco de dados remoto.
* `user` — Usuário MySQL.
* `password` — Senha do usuário.

**Configurações**

<div id="enable-compression">
  ### `enable_compression`
</div>

Ativa a compressão zlib para a conexão do protocolo MySQL. Quando definido como `1`, o ClickHouse solicita a compressão no nível do protocolo ao servidor MySQL.

Valor padrão: `0`.

Exemplo:

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

<div id="data_types-support">
  ## Suporte a tipos de dados
</div>

| MySQL                            | ClickHouse                                                   |
| -------------------------------- | ------------------------------------------------------------ |
| UNSIGNED TINYINT                 | [UInt8](../../sql-reference/data-types/int-uint.md)          |
| TINYINT                          | [Int8](../../sql-reference/data-types/int-uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| SMALLINT                         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| BIGINT                           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| FLOAT                            | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                           | [Float64](../../sql-reference/data-types/float.md)           |
| DATE                             | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP              | [DateTime](../../sql-reference/data-types/datetime.md)       |
| BINARY                           | [FixedString](../../sql-reference/data-types/fixedstring.md) |

Todos os demais tipos de dados do MySQL são convertidos para [String](../../sql-reference/data-types/string.md).

Há suporte para [Nullable](../../sql-reference/data-types/nullable.md).

<div id="global-variables-support">
  ## Suporte a variáveis globais
</div>

Para maior compatibilidade, você pode referenciar variáveis globais no estilo do MySQL, como `@@identifier`.

Há suporte para estas variáveis:

* `version`
* `max_allowed_packet`

:::note
No momento, essas variáveis são apenas stubs e não correspondem a nada.
:::

Exemplo:

```sql
SELECT @@version;
```

<div id="examples-of-use">
  ## Exemplos de uso
</div>

Tabela no MySQL:

```text
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

Banco de dados no ClickHouse que troca dados com o servidor MySQL:

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password') SETTINGS read_write_timeout=10000, connect_timeout=100;
```

```sql
SHOW DATABASES
```

```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

```sql
SHOW TABLES FROM mysql_db
```

```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

```sql
SELECT * FROM mysql_db.mysql_table
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```