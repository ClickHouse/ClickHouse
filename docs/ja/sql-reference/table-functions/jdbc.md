---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: jdbc
---

# jdbc {#table-function-jdbc}

`jdbc(datasource, schema, table)` -JDBCドライバ経由で接続されたテーブルを返します。

このテーブル関数には、別々の [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) 実行するプログラム。
Null許容型をサポートします(照会されるリモートテーブルのDDLに基づきます)。

**例**

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

[元の記事](https://clickhouse.com/docs/en/query_language/table_functions/jdbc/) <!--hide-->
