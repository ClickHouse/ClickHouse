---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 43
toc_title: jdbc
---

# jdbc {#table-function-jdbc}

`jdbc(jdbc_connection_uri, schema, table)` -JDBCドライバ経由で接続されたテーブルを返します。

このテーブル関数には、別々の `clickhouse-jdbc-bridge` 実行するプログラム。
Null許容型をサポートします(照会されるリモートテーブルのDDLに基づきます)。

**例**

``` sql
SELECT * FROM jdbc('jdbc:mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('mysql://localhost:3306/?user=root&password=root', 'schema', 'table')
```

``` sql
SELECT * FROM jdbc('datasource://mysql-local', 'schema', 'table')
```

[元の記事](https://clickhouse.tech/docs/en/query_language/table_functions/jdbc/) <!--hide-->
