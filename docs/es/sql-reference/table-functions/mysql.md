---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: mysql
---

# mysql {#mysql}

Permitir `SELECT` consultas que se realizarán en los datos que se almacenan en un servidor MySQL remoto.

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Parámetros**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` consultas a `REPLACE INTO`. Si `replace_query=1`, la consulta se reemplaza.

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expresión que se añade a la `INSERT` consulta.

        Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the MySQL documentation to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.

        To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Simple `WHERE` cláusulas tales como `=, !=, >, >=, <, <=` se ejecutan actualmente en el servidor MySQL.

El resto de las condiciones y el `LIMIT` La restricción de muestreo se ejecuta en ClickHouse solo después de que finalice la consulta a MySQL.

**Valor devuelto**

Un objeto de tabla con las mismas columnas que la tabla MySQL original.

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

Selección de datos de ClickHouse:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Ver también {#see-also}

-   [El ‘MySQL’ motor de mesa](../../engines/table-engines/integrations/mysql.md)
-   [Uso de MySQL como fuente de diccionario externo](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/mysql/) <!--hide-->
