---
description: 'Permite ejecutar consultas `SELECT` e `INSERT` en datos almacenados
  en un servidor MySQL remoto.'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

Permite ejecutar consultas `SELECT` e `INSERT` en datos almacenados en un servidor MySQL remoto.

<div id="syntax">
  ## Sintaxis
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento             | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`           | Dirección del servidor MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `database`            | Nombre de la base de datos remota.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `table`               | Nombre de la tabla remota o una consulta que se pasa a MySQL tal cual (consulte [Pasar una consulta en lugar de un nombre de tabla](#passing-a-query)).                                                                                                                                                                                                                                                                                                                                                                                   |
| `user`                | Usuario MySQL.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `password`            | Contraseña del usuario.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `replace_query`       | Indicador que convierte las consultas `INSERT INTO` en `REPLACE INTO`. Valores posibles:<br />    - `0` - La consulta se ejecuta como `INSERT INTO`.<br />    - `1` - La consulta se ejecuta como `REPLACE INTO`.                                                                                                                                                                                                                                                                                                                         |
| `on_duplicate_clause` | La expresión `ON DUPLICATE KEY on_duplicate_clause` que se añade a la consulta `INSERT`. Solo puede especificarse con `replace_query = 0` (si se pasan simultáneamente `replace_query = 1` y `on_duplicate_clause`, ClickHouse genera una excepción).<br />    Ejemplo: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    Aquí, `on_duplicate_clause` es `UPDATE c2 = c2 + 1`. Consulte la documentación de MySQL para saber qué `on_duplicate_clause` puede usar con la cláusula `ON DUPLICATE KEY`. |

Los argumentos también pueden pasarse mediante [colecciones con nombre](/es/operations/named-collections.md). En este caso, `host` y `port` deben especificarse por separado. Este enfoque se recomienda para entornos de producción.

Las cláusulas `WHERE` simples, como `=, !=, >, >=, <, <=`, se ejecutan actualmente en el servidor MySQL.

El resto de las condiciones y la restricción de muestreo `LIMIT` se ejecutan en ClickHouse solo después de que finaliza la consulta a MySQL.

<div id="passing-a-query">
  ## Usar una consulta en lugar de un nombre de tabla
</div>

En lugar de un nombre de tabla, el tercer argumento puede ser una consulta `SELECT` que se pasa a MySQL tal cual. La estructura de la tabla resultante se infiere a partir del resultado de la consulta. La consulta puede escribirse como una subconsulta o incluirse en la función `query`:

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

Esto es útil para hacer pushdown de joins, agregaciones o cualquier otro procesamiento a MySQL. Dicha tabla es de solo lectura: no se permite hacer `INSERT` en ella. La misma sintaxis es compatible con el motor de tabla [`MySQL`](/es/engines/table-engines/integrations/mysql).

:::note
La forma de subconsulta `(SELECT ...)` es analizada por ClickHouse y serializada de nuevo en el dialecto de MySQL (entrecomillado de identificadores con acento grave) antes de enviarse al servidor. Por lo tanto, debe ser válida en ClickHouse SQL. Para pasar sintaxis específica de MySQL que ClickHouse no analiza, usa la forma `query('...')`, cuyo texto se envía a MySQL literalmente.

Cualquier `WHERE`, `LIMIT`, agregación, etc. externo de la consulta de ClickHouse circundante **no** se hace pushdown en la consulta pasada; se aplica en ClickHouse después de recuperar el resultado completo de la consulta. Para restringir los datos leídos desde MySQL, coloca el filtro dentro de la consulta pasada. Con [`external_table_strict_query = 1`](/es/operations/settings/settings#external_table_strict_query), un filtro externo que no puede hacerse pushdown se rechaza con una excepción en lugar de aplicarse localmente.
:::

Admite múltiples réplicas que deben listarse con `|`. Por ejemplo:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

o

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tabla con las mismas columnas que la tabla original de MySQL.

:::note
Algunos tipos de datos de MySQL pueden mapearse a distintos tipos de ClickHouse; esto se controla mediante la configuración a nivel de consulta [mysql&#95;datatypes&#95;support&#95;level](/es/operations/settings/settings.md#mysql_datatypes_support_level)
:::

:::note
En la consulta `INSERT`, para distinguir la función de tabla `mysql(...)` de un nombre de tabla con una lista de nombres de columnas, debe usar las palabras clave `FUNCTION` o `TABLE FUNCTION`. Consulte los ejemplos a continuación.
:::

<div id="examples">
  ## Ejemplos
</div>

Tabla en MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

Consulta de datos en ClickHouse:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

O bien usando [colecciones con nombre](/es/operations/named-collections.md):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

Habilita la compresión para la conexión del protocolo MySQL.

Valor predeterminado: `false`.

Esta configuración se aplica a:

* la función de tabla `mysql`;
* el motor de tabla `MySQL`;
* el motor de base de datos `MySQL`;
* las colecciones con nombre utilizadas por las integraciones de MySQL.

Cuando está habilitada, ClickHouse solicita compresión para la conexión.

Ejemplo:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

Reemplazar e insertar:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

Copiar datos desde una tabla de MySQL a una tabla de ClickHouse:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

O si solo copia un lote incremental desde MySQL en función del id máximo actual:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## Relacionado
</div>

* [El motor de tabla &#39;MySQL&#39;](../../engines/table-engines/integrations/mysql.md)
* [Uso de MySQL como fuente de diccionario](/es/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/es/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/es/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/es/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/es/operations/settings/settings.md#mysql_max_rows_to_insert)