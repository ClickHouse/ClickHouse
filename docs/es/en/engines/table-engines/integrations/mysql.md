---
description: 'Documentación del motor de tabla de MySQL'
sidebar_label: 'MySQL'
sidebar_position: 138
slug: /engines/table-engines/integrations/mysql
title: 'Motor de tabla de MySQL'
doc_type: 'reference'
---

El motor MySQL le permite realizar consultas `SELECT` e `INSERT` sobre datos almacenados en un servidor MySQL remoto.

<div id="creating-a-table">
  ## Crear una tabla
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

Consulte una descripción detallada de la consulta [CREATE TABLE](/es/sql-reference/statements/create/table).

La estructura de la tabla puede diferir de la de la tabla MySQL original:

* Los nombres de las columnas deben ser los mismos que en la tabla MySQL original, pero puede usar solo algunas de ellas y en cualquier orden.
* Los tipos de las columnas pueden diferir de los de la tabla MySQL original. ClickHouse intenta [convertir](../../../engines/database-engines/mysql.md#data_types-support) los valores a los tipos de datos de ClickHouse.
* La configuración [external&#95;table&#95;functions&#95;use&#95;nulls](/es/operations/settings/settings#external_table_functions_use_nulls) define cómo se gestionan las columnas Nullable. Valor predeterminado: 1. Si es 0, la función de tabla no crea columnas Nullable e inserta valores predeterminados en lugar de NULL. Esto también se aplica a los valores NULL dentro de arrays.

**Parámetros del motor**

* `host:port` — Dirección del servidor MySQL.
* `database` — Nombre de la base de datos remota.
* `table` — Nombre de la tabla remota, o una consulta que se pasa a MySQL tal cual (consulte [Pasar una consulta en lugar de un nombre de tabla](#passing-a-query)).
* `user` — usuario MySQL.
* `password` — Contraseña del usuario.
* `replace_query` — Indicador que convierte las consultas `INSERT INTO` en `REPLACE INTO`. Si `replace_query=1`, la consulta se sustituye.
* `on_duplicate_clause` — La expresión `ON DUPLICATE KEY on_duplicate_clause` que se añade a la consulta `INSERT`.
  Ejemplo: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, donde `on_duplicate_clause` es `UPDATE c2 = c2 + 1`. Consulte la [documentación de MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) para ver qué `on_duplicate_clause` puede usar con la cláusula `ON DUPLICATE KEY`.
  Para especificar `on_duplicate_clause`, debe pasar `0` en el parámetro `replace_query`. Si pasa simultáneamente `replace_query = 1` y `on_duplicate_clause`, ClickHouse genera una excepción.

Los argumentos también pueden pasarse mediante [colecciones con nombre](/es/operations/named-collections.md). En este caso, `host` y `port` deben especificarse por separado. Este enfoque se recomienda para entornos de producción.

Las cláusulas `WHERE` simples, como `=, !=, >, >=, <, <=`, se ejecutan en el servidor MySQL.

El resto de las condiciones y la restricción de muestreo `LIMIT` se ejecutan en ClickHouse solo después de que finaliza la consulta a MySQL.

<div id="passing-a-query">
  ## Pasar una consulta en lugar de un nombre de tabla
</div>

En lugar de un nombre de tabla, el argumento `table` puede ser una consulta `SELECT` que se pasa a MySQL tal cual. La estructura de la tabla se infiere a partir del resultado de la consulta. La consulta puede escribirse como una subconsulta o ir envuelta en la función `query`:

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Esto es útil para hacer pushdown de joins, agregaciones o cualquier otro procesamiento a MySQL. Esta tabla es de solo lectura: no se permite hacer `INSERT` en ella. La misma sintaxis también es compatible con la función de tabla [`mysql`](/es/sql-reference/table-functions/mysql).

:::note
La forma de subconsulta `(SELECT ...)` la analiza ClickHouse y luego la vuelve a serializar en el dialecto de MySQL (comillas invertidas para los identificadores) antes de enviarla al server. Por lo tanto, debe ser válida en ClickHouse SQL. Para pasar sintaxis específica de MySQL que ClickHouse no analiza, use la forma `query('...')`, cuyo texto se envía literalmente a MySQL.

Cualquier `WHERE`, `LIMIT`, agregación, etc. externos de la consulta de ClickHouse circundante **no** se hacen pushdown en la consulta proporcionada, sino que se aplican en ClickHouse después de recuperar el resultado completo de la consulta. Para restringir los datos leídos desde MySQL, coloque el filtro dentro de la consulta proporcionada. Con [`external_table_strict_query = 1`](/es/operations/settings/settings#external_table_strict_query), un filtro externo que no puede hacerse pushdown se rechaza con una excepción en lugar de aplicarse localmente.
:::

Admite varias réplicas, que deben enumerarse con `|`. Por ejemplo:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

<div id="usage-example">
  ## Ejemplo de uso
</div>

Cree una tabla en MySQL:

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

Crear una tabla en ClickHouse con argumentos simples:

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

O usando [colecciones con nombre](/es/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Obtención de datos de una tabla de MySQL:

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

<div id="mysql-settings">
  ## Configuración
</div>

Los ajustes predeterminados no son muy eficientes, ya que ni siquiera reutilizan las conexiones. Estos ajustes permiten aumentar el número de consultas que el servidor ejecuta por segundo.

<div id="connection-auto-close">
  ### `connection_auto_close`
</div>

Permite cerrar automáticamente la conexión después de ejecutar la consulta, es decir, desactivar la reutilización de la conexión.

Valores posibles:

* 1 — Se permite cerrar automáticamente la conexión, por lo que la reutilización de la conexión queda desactivada
* 0 — No se permite cerrar automáticamente la conexión, por lo que la reutilización de la conexión queda activada

Valor predeterminado: `1`.

<div id="connection-max-tries">
  ### `connection_max_tries`
</div>

Establece el número de reintentos del grupo con failover.

Posibles valores:

* Un entero positivo.
* 0 — No hay reintentos para el grupo con failover.

Valor predeterminado: `3`.

<div id="connection-pool-size">
  ### `connection_pool_size`
</div>

Tamaño del grupo de conexiones (si todas las conexiones están en uso, la consulta esperará hasta que se libere alguna).

Posibles valores:

* Entero positivo.

Valor predeterminado: `16`.

<div id="connection-wait-timeout">
  ### `connection_wait_timeout`
</div>

Tiempo de espera (en segundos) para esperar a que haya una conexión libre (en caso de que ya haya `connection_pool_size` conexiones activas); 0: no esperar.

Valores posibles:

* Entero positivo.

Valor predeterminado: `5`.

<div id="connect-timeout">
  ### `connect_timeout`
</div>

Tiempo de espera de conexión (en segundos).

Valores posibles:

* Entero positivo.

Valor predeterminado: `10`.

<div id="read-write-timeout">
  ### `read_write_timeout`
</div>

Tiempo de espera de lectura y escritura (en segundos).

Posibles valores:

* Entero positivo.

Valor predeterminado: `300`.

<div id="enable-compression">
  ### `enable_compression`
</div>

Habilita la compresión para la conexión mediante el protocolo MySQL.

Valor predeterminado: `false`.

Esta configuración se aplica a:

* el motor de tabla `MySQL`;
* el motor de base de datos `MySQL`;
* la función de tabla `mysql`;
* las colecciones con nombre usadas por las integraciones de MySQL.

Cuando está habilitada, ClickHouse solicita compresión para la conexión.

Ejemplo:

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

<div id="see-also">
  ## Véase también
</div>

* [La función de tabla MySQL](../../../sql-reference/table-functions/mysql.md)
* [Uso de MySQL como fuente de diccionario](/es/sql-reference/statements/create/dictionary/sources/mysql)