---
machine_translated: true
---

# mysql {#mysql}

Permitir `SELECT` consultas que se realizarán en los datos que se almacenan en un servidor MySQL remoto.

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**Parámetros**

-   `host:port` — Dirección del servidor MySQL.

-   `database` — Nombre de base de datos remota.

-   `table` — Nombre de la tabla remota.

-   `user` — Usuario de MySQL.

-   `password` — Contraseña de usuario.

-   `replace_query` — Bandera que convierte `INSERT INTO` Consultas a `REPLACE INTO`. Si `replace_query=1`, la consulta se reemplaza.

-   `on_duplicate_clause` — El `ON DUPLICATE KEY on_duplicate_clause` expresión que se añade a la `INSERT` consulta.

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
+--------+--------------+-------+----------------+
| int_id | int_nullable | float | float_nullable |
+--------+--------------+-------+----------------+
|      1 |         NULL |     2 |           NULL |
+--------+--------------+-------+----------------+
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

-   [El ‘MySQL’ motor de mesa](../../operations/table_engines/mysql.md)
-   [Uso de MySQL como fuente de diccionario externo](../dicts/external_dicts_dict_sources.md#dicts-external_dicts_dict_sources-mysql)

[Artículo Original](https://clickhouse.tech/docs/es/query_language/table_functions/mysql/) <!--hide-->
