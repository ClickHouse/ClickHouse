---
slug: /ja/engines/table-engines/integrations/ExternalDistributed
sidebar_position: 55
sidebar_label: ExternalDistributed
title: ExternalDistributed
---

`ExternalDistributed`エンジンは、リモートサーバーのMySQLまたはPostgreSQLに保存されているデータに対して`SELECT`クエリを実行することを可能にします。引数として[MySQL](../../../engines/table-engines/integrations/mysql.md)または[PostgreSQL](../../../engines/table-engines/integrations/postgresql.md)エンジンを受け入れ、シャードが可能です。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = ExternalDistributed('engine', 'host:port', 'database', 'table', 'user', 'password');
```

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query)クエリの詳細な説明を参照してください。

テーブル構造は元のテーブル構造と異なることがあります：

- カラム名は元のテーブルと同じでなければなりませんが、これらのカラムの一部のみを任意の順序で利用できます。
- カラム型は元のテーブルと異なることがあります。ClickHouseは値をClickHouseのデータ型に[キャスト](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast)しようとします。

**エンジンパラメータ**

- `engine` — テーブルエンジン `MySQL` または `PostgreSQL`。
- `host:port` — MySQLまたはPostgreSQLサーバーアドレス。
- `database` — リモートデータベース名。
- `table` — リモートテーブル名。
- `user` — ユーザー名。
- `password` — ユーザーパスワード。

## 実装の詳細 {#implementation-details}

複数のレプリカをサポートしており、`|`でリストし、シャードは`,`でリストする必要があります。例：

```sql
CREATE TABLE test_shards (id UInt32, name String, age UInt32, money UInt32) ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

レプリカを指定する際には、読み込み時にシャードごとに利用可能なレプリカの一つが選択されます。接続が失敗した場合、次のレプリカが選択され、この処理はすべてのレプリカに対して行われます。すべてのレプリカで接続試行が失敗した場合、同じ方法で何度か試行が繰り返されます。

任意の数のシャードと各シャードに対する任意の数のレプリカを指定できます。

**関連項目**

- [MySQL テーブルエンジン](../../../engines/table-engines/integrations/mysql.md)
- [PostgreSQL テーブルエンジン](../../../engines/table-engines/integrations/postgresql.md)
- [分散テーブルエンジン](../../../engines/table-engines/special/distributed.md)
