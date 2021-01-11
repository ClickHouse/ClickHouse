# jdbc {#jdbc}

`jdbc(jdbc_connection_uri, schema, table)` - возвращает таблицу, соединение с которой происходит через JDBC-драйвер.

Для работы этой табличной функции требуется отдельно запускать приложение clickhouse-jdbc-bridge.
Данная функция поддерживает Nullable типы (на основании DDL таблицы к которой происходит запрос).

**Пример**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('datasource://mysql-local', 'schema', 'table')
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/table_functions/jdbc/) <!--hide-->
