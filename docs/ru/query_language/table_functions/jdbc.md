<a name="table_functions-jdbc"></a>

# jdbc

`jdbc(jdbc_connection_uri, schema, table)` - возвращает таблицу, соединение с которой происходит через JDBC-драйвер.

Для работы этой табличной функциии требуется отдельно запускать приложение clickhouse-jdbc-bridge.
В отличии от табличной функции `odbc`, данная функция поддерживает Nullable типы (на основании DDL таблицы к которой происходит запрос).


**Пример**

```sql
SELECT * FROM url('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

```sql
SELECT * FROM url('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

```sql
SELECT * FROM url('datasource://mysql-local', 'schema', 'table')
```