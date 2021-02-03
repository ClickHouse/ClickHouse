---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: jdbc
---

# jdbc {#table-function-jdbc}

`jdbc(jdbc_connection_uri, schema, table)` - devuelve la tabla que está conectado a través del controlador JDBC.

Esta función de tabla requiere `clickhouse-jdbc-bridge` programa para estar en ejecución.
Admite tipos Nullable (basados en DDL de la tabla remota que se consulta).

**Ejemplos**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('datasource://mysql-local', 'schema', 'table')
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
