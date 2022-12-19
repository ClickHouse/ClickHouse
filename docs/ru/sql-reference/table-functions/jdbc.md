---
sidebar_position: 43
sidebar_label: jdbc
---

# jdbc {#jdbc}

`jdbc(datasource, schema, table)` - возвращает таблицу, соединение с которой происходит через JDBC-драйвер.

Для работы этой табличной функции требуется отдельно запускать приложение [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge).
Данная функция поддерживает Nullable типы (на основании DDL таблицы к которой происходит запрос).

**Пример**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'select * from schema.table')
```

``` sql
SELECT * FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

``` sql
SELECT *
FROM jdbc('mysql-dev?p1=233', 'num Int32', 'select toInt32OrZero(''{{p1}}'') as num')
```

``` sql
SELECT a.datasource AS server1, b.datasource AS server2, b.name AS db
FROM jdbc('mysql-dev?datasource_column', 'show databases') a
INNER JOIN jdbc('self?datasource_column', 'show databases') b ON a.Database = b.name
```

[Оригинальная статья](https://clickhouse.com/docs/en/query_language/table_functions/jdbc/) <!--hide-->
