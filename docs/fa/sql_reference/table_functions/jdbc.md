---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 43
---

# جستجو {#table-function-jdbc}

`jdbc(jdbc_connection_uri, schema, table)` - جدول بازده است که از طریق راننده جدی بی سی متصل.

این تابع جدول نیاز به جداگانه دارد `clickhouse-jdbc-bridge` برنامه در حال اجرا است.
این پشتیبانی از انواع باطل (بر اساس دسیدال جدول از راه دور است که تردید).

**مثالها**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('datasource://mysql-local', 'schema', 'table')
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
