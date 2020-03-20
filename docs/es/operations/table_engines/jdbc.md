# JDBC {#table-engine-jdbc}

Permite que ClickHouse se conecte a bases de datos externas a través de [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity).

Para implementar la conexión JDBC, ClickHouse utiliza el programa independiente [Sistema abierto.](https://github.com/alex-krash/clickhouse-jdbc-bridge) que debería ejecutarse como un demonio.

Este motor soporta el [NULL](../../data_types/nullable.md) tipo de datos.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name
(
    columns list...
)
ENGINE = JDBC(dbms_uri, external_database, external_table)
```

**Parámetros del motor**

-   `dbms_uri` — URI de un DBMS externo.

    Formato: `jdbc:<driver_name>://<host_name>:<port>/?user=<username>&password=<password>`.
    Ejemplo para MySQL: `jdbc:mysql://localhost:3306/?user=root&password=root`.

-   `external_database` — Base de datos en un DBMS externo.

-   `external_table` — Nombre de la tabla en `external_database`.

## Ejemplo de uso {#usage-example}

Creación de una tabla en el servidor MySQL mediante la conexión directa con su cliente de consola:

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

Creación de una tabla en el servidor ClickHouse y selección de datos de ella:

``` sql
CREATE TABLE jdbc_table
(
    `int_id` Int32,
    `int_nullable` Nullable(Int32),
    `float` Float32,
    `float_nullable` Nullable(Float32)
)
ENGINE JDBC('jdbc:mysql://localhost:3306/?user=root&password=root', 'test', 'test')
```

``` sql
SELECT *
FROM jdbc_table
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

## Ver también {#see-also}

-   [Función de la tabla de JDBC](../../query_language/table_functions/jdbc.md).

[Artículo Original](https://clickhouse.tech/docs/es/operations/table_engines/jdbc/) <!--hide-->
