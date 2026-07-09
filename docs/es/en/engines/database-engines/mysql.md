---
description: 'Permite conectarse a bases de datos en un servidor MySQL remoto y ejecutar
  consultas `INSERT` y `SELECT` para intercambiar datos entre ClickHouse y MySQL.'
sidebar_label: 'MySQL'
sidebar_position: 50
slug: /engines/database-engines/mysql
title: 'MySQL'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mysql-database-engine">
  # Motor de base de datos MySQL
</div>

<CloudNotSupportedBadge />

Permite conectarse a bases de datos en un servidor MySQL remoto y realizar consultas `INSERT` y `SELECT` para intercambiar datos entre ClickHouse y MySQL.

El motor de base de datos `MySQL` traduce las consultas para el servidor MySQL, de modo que pueda realizar operaciones como `SHOW TABLES` o `SHOW CREATE TABLE`.

No se pueden realizar las siguientes consultas:

* `RENAME`
* `CREATE TABLE`
* `ALTER`

<div id="creating-a-database">
  ## Crear una base de datos
</div>

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', ['database' | database], 'user', 'password')
[SETTINGS enable_compression=0]
```

**Parámetros del motor**

* `host:port` — Dirección del servidor MySQL.
* `database` — Nombre de la base de datos remota.
* `user` — Usuario MySQL.
* `password` — Contraseña del usuario.

**Configuración**

<div id="enable-compression">
  ### `enable_compression`
</div>

Habilita la compresión zlib para la conexión del protocolo MySQL. Cuando se establece en `1`, ClickHouse solicita compresión a nivel de protocolo al servidor MySQL.

Valor predeterminado: `0`.

Ejemplo:

```sql
CREATE DATABASE mysql_db
ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
SETTINGS enable_compression = 1;
```

<div id="data_types-support">
  ## Compatibilidad de tipos de datos
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

Todos los demás tipos de datos de MySQL se convierten en [String](../../sql-reference/data-types/string.md).

Se admite [Nullable](../../sql-reference/data-types/nullable.md).

<div id="global-variables-support">
  ## Compatibilidad con variables globales
</div>

Para una mejor compatibilidad, puede hacer referencia a las variables globales al estilo de MySQL, como `@@identifier`.

Se admiten estas variables:

* `version`
* `max_allowed_packet`

:::note
Por ahora, estas variables son solo marcadores de posición y no se corresponden con nada.
:::

Ejemplo:

```sql
SELECT @@version;
```

<div id="examples-of-use">
  ## Ejemplos de uso
</div>

Tabla en MySQL:

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

Base de datos en ClickHouse que intercambia datos con el servidor MySQL:

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