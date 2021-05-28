---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 44
toc_title: Nosotros
---

# Nosotros {#table-functions-odbc}

Devuelve la tabla que está conectada a través de [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

``` sql
odbc(connection_settings, external_database, external_table)
```

Parámetros:

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

Para implementar con seguridad conexiones ODBC, ClickHouse usa un programa separado `clickhouse-odbc-bridge`. Si el controlador ODBC se carga directamente desde `clickhouse-server`, problemas de controlador pueden bloquear el servidor ClickHouse. ClickHouse se inicia automáticamente `clickhouse-odbc-bridge` cuando se requiere. El programa de puente ODBC se instala desde el mismo paquete que el `clickhouse-server`.

Los campos con el `NULL` Los valores de la tabla externa se convierten en los valores predeterminados para el tipo de datos base. Por ejemplo, si un campo de tabla MySQL remoto tiene `INT NULL` tipo se convierte a 0 (el valor predeterminado para ClickHouse `Int32` tipo de datos).

## Ejemplo de uso {#usage-example}

**Obtener datos de la instalación local de MySQL a través de ODBC**

Este ejemplo se comprueba para Ubuntu Linux 18.04 y el servidor MySQL 5.7.

Asegúrese de que unixODBC y MySQL Connector están instalados.

De forma predeterminada (si se instala desde paquetes), ClickHouse comienza como usuario `clickhouse`. Por lo tanto, debe crear y configurar este usuario en el servidor MySQL.

``` bash
$ sudo mysql
```

``` sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

A continuación, configure la conexión en `/etc/odbc.ini`.

``` bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Puede verificar la conexión usando el `isql` utilidad desde la instalación de unixODBC.

``` bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

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

Recuperación de datos de la tabla MySQL en ClickHouse:

``` sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

## Ver también {#see-also}

-   [Diccionarios externos ODBC](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [Motor de tabla ODBC](../../engines/table-engines/integrations/odbc.md).

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
