---
description: 'Devuelve la tabla conectada mediante ODBC.'
sidebar_label: 'odbc'
sidebar_position: 150
slug: /sql-reference/table-functions/odbc
title: 'odbc'
doc_type: 'reference'
---

Devuelve la tabla conectada mediante [ODBC](https://en.wikipedia.org/wiki/Open_Database_Connectivity).

<div id="syntax">
  ## Sintaxis
</div>

```sql
odbc(datasource, external_database, external_table)
odbc(datasource, external_table)
odbc(named_collection)
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento           | Descripción                                                                        |
| ------------------- | ---------------------------------------------------------------------------------- |
| `datasource`        | Nombre de la sección con la configuración de la conexión en el archivo `odbc.ini`. |
| `external_database` | Nombre de una base de datos en un DBMS externo.                                    |
| `external_table`    | Nombre de una tabla en `external_database`.                                        |

Estos parámetros también se pueden pasar mediante [colecciones con nombre](/es/operations/named-collections.md).

Para implementar conexiones ODBC de forma segura, ClickHouse utiliza un programa independiente: `clickhouse-odbc-bridge`. Si el controlador ODBC se carga directamente desde `clickhouse-server`, los problemas del controlador pueden hacer que el servidor de ClickHouse falle. ClickHouse inicia automáticamente `clickhouse-odbc-bridge` cuando es necesario. El programa ODBC bridge se instala desde el mismo paquete que `clickhouse-server`.

Los campos con valores `NULL` de la tabla externa se convierten en los valores predeterminados del tipo de dato subyacente. Por ejemplo, si un campo de una tabla MySQL remota tiene el tipo `INT NULL`, se convierte en 0 (el valor predeterminado del tipo de dato `Int32` de ClickHouse).

<div id="usage-example">
  ## Ejemplo de uso
</div>

**Obtención de datos de la instalación local de MySQL mediante ODBC**

Este ejemplo se ha probado en Ubuntu Linux 18.04 y MySQL server 5.7.

Asegúrese de que unixODBC y MySQL Connector estén instalados.

De forma predeterminada (si se instala desde paquetes), ClickHouse se inicia con el usuario `clickhouse`. Por lo tanto, debe crear y configurar este usuario en el servidor MySQL.

```bash
$ sudo mysql
```

```sql
mysql> CREATE USER 'clickhouse'@'localhost' IDENTIFIED BY 'clickhouse';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'clickhouse'@'clickhouse' WITH GRANT OPTION;
```

A continuación, configure la conexión en `/etc/odbc.ini`.

```bash
$ cat /etc/odbc.ini
[mysqlconn]
DRIVER = /usr/local/lib/libmyodbc5w.so
SERVER = 127.0.0.1
PORT = 3306
DATABASE = test
USERNAME = clickhouse
PASSWORD = clickhouse
```

Puede comprobar la conexión con la utilidad `isql` de la instalación de unixODBC.

```bash
$ isql -v mysqlconn
+-------------------------+
| Connected!                            |
|                                       |
...
```

Tabla en MySQL:

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

Recuperación de datos de la tabla de MySQL en ClickHouse:

```sql
SELECT * FROM odbc('DSN=mysqlconn', 'test', 'test')
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │            0 │     2 │              0 │
└────────┴──────────────┴───────┴────────────────┘
```

<div id="see-also">
  ## Véase también
</div>

* [Diccionarios ODBC](/es/sql-reference/statements/create/dictionary/sources/odbc)
* [motor de tabla ODBC](/es/engines/table-engines/integrations/odbc).