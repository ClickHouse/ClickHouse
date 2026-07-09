---
description: '`ExternalDistributed` エンジンを使用すると、リモートサーバー上の MySQL または PostgreSQL に格納されているデータに対して `SELECT` クエリを実行できます。引数に MySQL または PostgreSQL エンジンを指定できるため、シャーディングが可能です。'
sidebar_label: 'ExternalDistributed'
sidebar_position: 55
slug: /engines/table-engines/integrations/ExternalDistributed
title: 'ExternalDistributed テーブルエンジン'
doc_type: 'reference'
---

`ExternalDistributed` エンジンを使用すると、リモートサーバー上の MySQL または PostgreSQL に格納されているデータに対して `SELECT` クエリを実行できます。引数に [MySQL](../../../engines/table-engines/integrations/mysql.md) または [PostgreSQL](../../../engines/table-engines/integrations/postgresql.md) エンジンを指定できるため、シャーディングが可能です。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳細な説明を参照してください。

テーブル構造は、元のテーブル構造と異なる場合があります。

* カラム名は元のテーブルと同じである必要がありますが、それらの一部のカラムだけを任意の順序で使用できます。
* カラム型は元のテーブルのものと異なる場合があります。ClickHouse は値を ClickHouse データ型に [変換](/ja/sql-reference/functions/type-conversion-functions#CAST) しようとします。

**エンジンパラメータ**

* `engine` — テーブルエンジン `MySQL` または `PostgreSQL`。
* `host:port` — MySQL または PostgreSQL サーバーのアドレス。
* `database` — リモートデータベース名。
* `table` — リモートテーブル名。
* `user` — ユーザー名。
* `password` — ユーザーパスワード。

<div id="implementation-details">
  ## 実装の詳細
</div>

複数のレプリカに対応しており、レプリカは `|`、分片は `,` で区切って列挙する必要があります。例えば:

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

レプリカを指定すると、読み取り時には各分片ごとに利用可能なレプリカの中から 1 つが選択されます。接続に失敗した場合は次のレプリカが選択され、すべてのレプリカが試されるまでこの処理が続けられます。すべてのレプリカへの接続試行が失敗した場合は、同じ方法で数回再試行されます。

分片の数は任意に指定でき、各分片に対するレプリカの数も任意に指定できます。

**関連項目**

* [MySQL テーブルエンジン](../../../engines/table-engines/integrations/mysql.md)
* [PostgreSQL テーブルエンジン](../../../engines/table-engines/integrations/postgresql.md)
* [Distributed テーブルエンジン](../../../engines/table-engines/special/distributed.md)