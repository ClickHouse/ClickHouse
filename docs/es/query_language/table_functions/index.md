---
machine_translated: true
---

# Funciones de tabla {#table-functions}

Las funciones de tabla son métodos para construir tablas.

Puede usar funciones de tabla en:

-   [DE](../select.md#select-from) cláusula de la `SELECT` consulta.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [Crear TABLA COMO \<table\_function() \>](../create.md#create-table-query) consulta.

        It's one of the methods of creating a table.

!!! warning "Advertencia"
    No puede utilizar funciones de tabla si [Método de codificación de datos:](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) la configuración está deshabilitada.

| Función              | Descripción                                                                                                                       |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| [file](file.md)      | Crea un [File](../../operations/table_engines/file.md)-mesa del motor.                                                            |
| [Fusionar](merge.md) | Crea un [Fusionar](../../operations/table_engines/merge.md)-mesa del motor.                                                       |
| [número](numbers.md) | Crea una tabla con una sola columna llena de números enteros.                                                                     |
| [remoto](remote.md)  | Le permite acceder a servidores remotos sin crear un [Distribuido](../../operations/table_engines/distributed.md)-mesa del motor. |
| [URL](url.md)        | Crea un [URL](../../operations/table_engines/url.md)-mesa del motor.                                                              |
| [mysql](mysql.md)    | Crea un [MySQL](../../operations/table_engines/mysql.md)-mesa del motor.                                                          |
| [jdbc](jdbc.md)      | Crea un [JDBC](../../operations/table_engines/jdbc.md)-mesa del motor.                                                            |
| [Nosotros](odbc.md)  | Crea un [ODBC](../../operations/table_engines/odbc.md)-mesa del motor.                                                            |
| [Hdfs](hdfs.md)      | Crea un [HDFS](../../operations/table_engines/hdfs.md)-mesa del motor.                                                            |

[Artículo Original](https://clickhouse.tech/docs/es/query_language/table_functions/) <!--hide-->
