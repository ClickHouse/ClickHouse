---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: ODBC
---

# ODBC {#table-engine-odbc}

Permite que ClickHouse se conecte a bases de datos externas a través de [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

Para implementar con seguridad conexiones ODBC, ClickHouse usa un programa separado `clickhouse-odbc-bridge`. Si el controlador ODBC se carga directamente desde `clickhouse-server`, problemas de controlador pueden bloquear el servidor ClickHouse. ClickHouse se inicia automáticamente `clickhouse-odbc-bridge` cuando se requiere. El programa de puente ODBC se instala desde el mismo paquete que el `clickhouse-server`.

Este motor soporta el [NULL](../../../sql-reference/data-types/nullable.md) tipo de datos.

## Creación de una tabla {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
)
ENGINE = ODBC(connection_settings, external_database, external_table)
```

Vea una descripción detallada del [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) consulta.

La estructura de la tabla puede diferir de la estructura de la tabla de origen:

-   Los nombres de columna deben ser los mismos que en la tabla de origen, pero puede usar solo algunas de estas columnas y en cualquier orden.
-   Los tipos de columna pueden diferir de los de la tabla de origen. ClickHouse intenta [elenco](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) valores a los tipos de datos ClickHouse.

**Parámetros del motor**

-   `connection_settings` — Name of the section with connection settings in the `odbc.ini` file.
-   `external_database` — Name of a database in an external DBMS.
-   `external_table` — Name of a table in the `external_database`.

## Ejemplo de uso {#usage-example}

**Recuperación de datos de la instalación local de MySQL a través de ODBC**

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

Tabla en ClickHouse, recuperando datos de la tabla MySQL:

``` sql
CREATE TABLE odbc_t
(
    `int_id` Int32,
    `float_nullable` Nullable(Float32)
)
ENGINE = ODBC('DSN=mysqlconn', 'test', 'test')
```

``` sql
SELECT * FROM odbc_t
```

``` text
┌─int_id─┬─float_nullable─┐
│      1 │           ᴺᵁᴸᴸ │
└────────┴────────────────┘
```

## Ver también {#see-also}

-   [Diccionarios externos ODBC](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-odbc)
-   [Tabla ODBC función](../../../sql-reference/table-functions/odbc.md)

[Artículo Original](https://clickhouse.tech/docs/en/operations/table_engines/odbc/) <!--hide-->
