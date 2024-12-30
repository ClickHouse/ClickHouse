---
slug: /ja/sql-reference/table-functions/jdbc
sidebar_position: 100
sidebar_label: jdbc
---

# jdbc

:::note
clickhouse-jdbc-bridge はエクスペリメンタルなコードを含み、もはやサポートされていません。信頼性の問題やセキュリティの脆弱性を含む可能性があります。使用は自己責任で行ってください。
ClickHouse は、クリックハウス内の組み込みテーブル関数の使用を推奨します。これにより、アドホックなクエリシナリオ（Postgres、MySQL、MongoDB など）のためのより良い代替手段を提供します。
:::

`jdbc(datasource, schema, table)` - JDBC ドライバーを介して接続されるテーブルを返します。

このテーブル関数は、別途 [clickhouse-jdbc-bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge) プログラムの実行が必要です。
Nullable 型のサポート（クエリされたリモートテーブルの DDL に基づいて)も提供されています。

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

