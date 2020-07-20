---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

El motor MySQL le permite realizar `SELECT` consultas sobre datos almacenados en un servidor MySQL remoto.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

Vea una descripción detallada del [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) consulta.

La estructura de la tabla puede diferir de la estructura de la tabla MySQL original:

-   Los nombres de columna deben ser los mismos que en la tabla MySQL original, pero puede usar solo algunas de estas columnas y en cualquier orden.
-   Los tipos de columna pueden diferir de los de la tabla MySQL original. ClickHouse intenta [elenco](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) valores a los tipos de datos ClickHouse.

**Parámetros del motor**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` consultas a `REPLACE INTO`. Si `replace_query=1`, la consulta se sustituye.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expresión que se añade a la `INSERT` consulta.

    Ejemplo: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, donde `on_duplicate_clause` ser `UPDATE c2 = c2 + 1`. Ver el [Documentación de MySQL](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) para encontrar qué `on_duplicate_clause` se puede utilizar con el `ON DUPLICATE KEY` clausula.

    Especificar `on_duplicate_clause` tienes que pasar `0` a la `replace_query` parámetro. Si pasa simultáneamente `replace_query = 1` y `on_duplicate_clause`, ClickHouse genera una excepción.

Simple `WHERE` cláusulas tales como `=, !=, >, >=, <, <=` se ejecutan en el servidor MySQL.

El resto de las condiciones y el `LIMIT` La restricción de muestreo se ejecuta en ClickHouse solo después de que finalice la consulta a MySQL.

## Ejemplo de uso {#usage-example}

Tabla en MySQL:

``` text
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

Tabla en ClickHouse, recuperando datos de la tabla MySQL creada anteriormente:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Ver también {#see-also}

-   [El ‘mysql’ función de la tabla](../../../sql-reference/table-functions/mysql.md)
-   [Uso de MySQL como fuente de diccionario externo](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/mysql/) <!--hide-->
