---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
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
