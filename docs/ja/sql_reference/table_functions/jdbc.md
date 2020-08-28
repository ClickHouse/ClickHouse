---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 43
toc_title: jdbc
---

# jdbc {#table-function-jdbc}

`jdbc(jdbc_connection_uri, schema, table)` -JDBCドライバ経由で接続されたテーブルを返します。

このテーブル関数は、個別の `clickhouse-jdbc-bridge` 実行するプログラム。
でnullable種類に基づくddlのリモートテーブルが照会される).

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
