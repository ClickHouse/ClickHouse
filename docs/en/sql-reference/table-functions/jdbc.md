---
sidebar_position: 43
sidebar_label: jdbc
---

# jdbc

`jdbc(datasource, schema, table)` - returns table that is connected via JDBC driver.

This table function requires separate [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) program to be running.
It supports Nullable types (based on DDL of remote table that is queried).

**Examples**

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

[Original article](https://clickhouse.com/docs/en/query_language/table_functions/jdbc/) <!--hide-->
