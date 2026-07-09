---
description: 'MySQLテーブルエンジンのドキュメント'
sidebar_label: 'MySQL'
sidebar_position: 138
slug: /engines/table-engines/integrations/mysql
title: 'MySQL テーブルエンジン'
doc_type: 'reference'
---

MySQL テーブルエンジンを使用すると、リモートの MySQL サーバーに保存されているデータに対して `SELECT` および `INSERT` クエリを実行できます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

[CREATE TABLE](/ja/sql-reference/statements/create/table) クエリの詳細な説明を参照してください。

テーブル構造は、元の MySQL テーブルの構造と異なる場合があります。

* カラム名は元の MySQL テーブルと同じである必要がありますが、そのうち一部のカラムだけを任意の順序で使用できます。
* カラム型は元の MySQL テーブルのものと異なっていてもかまいません。ClickHouse は値を ClickHouse のデータ型に [型変換](../../../engines/database-engines/mysql.md#data_types-support) しようとします。
* [external&#95;table&#95;functions&#95;use&#95;nulls](/ja/operations/settings/settings#external_table_functions_use_nulls) 設定は、Nullable カラムの扱いを定義します。デフォルト値: 1。0 の場合、テーブル関数は Nullable カラムを作成せず、null の代わりにデフォルト値を挿入します。これは、配列内の NULL 値にも適用されます。

**エンジンパラメータ**

* `host:port` — MySQL サーバーのアドレス。
* `database` — リモートデータベース名。
* `table` — リモートテーブル名、またはそのまま MySQL に渡されるクエリ ([テーブル名の代わりにクエリを渡す](#passing-a-query) を参照) 。
* `user` — MySQL ユーザー。
* `password` — ユーザーのパスワード。
* `replace_query` — `INSERT INTO` クエリを `REPLACE INTO` に変換するフラグです。`replace_query=1` の場合、クエリは置き換えられます。
* `on_duplicate_clause` — `INSERT` クエリに追加される `ON DUPLICATE KEY on_duplicate_clause` 式。
  例: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`。この場合、`on_duplicate_clause` は `UPDATE c2 = c2 + 1` です。`ON DUPLICATE KEY` 句で使用できる `on_duplicate_clause` については、[MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) を参照してください。
  `on_duplicate_clause` を指定するには、`replace_query` パラメータに `0` を渡す必要があります。`replace_query = 1` と `on_duplicate_clause` を同時に渡すと、ClickHouse は例外を生成します。

引数は [named collections](/ja/operations/named-collections.md) を使用して渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。この方法は本番環境に推奨されます。

`=, !=, >, >=, <, <=` のような単純な `WHERE` 句は MySQL サーバー上で実行されます。

それ以外の条件と `LIMIT` のサンプリング制約は、MySQL へのクエリが完了した後でのみ ClickHouse で実行されます。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを渡す
</div>

テーブル名の代わりに、`table` 引数には、そのまま MySQL に渡される `SELECT` クエリを指定できます。テーブルの構造はクエリ結果から推論されます。クエリはサブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

これは、JOIN、集計、そのほかの処理を MySQL 側にプッシュダウンするのに役立ちます。このようなテーブルは読み取り専用で、これに対する `INSERT` は許可されません。同じ構文は、[`mysql`](/ja/sql-reference/table-functions/mysql) テーブル関数でもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は ClickHouse によってパースされ、サーバーに送信される前に MySQL 方言 (Identifier をバッククォートでクォートする形式) で再シリアライズされます。したがって、有効な ClickHouse SQL である必要があります。ClickHouse がパースしない MySQL 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式のテキストは、そのまま MySQL に送信されます。

周囲の ClickHouse クエリにある外側の `WHERE`、`LIMIT`、集計などは、渡されたクエリには**プッシュダウンされません**。これらは、完全なクエリ結果を取得した後に ClickHouse で適用されます。MySQL から読み取るデータを制限するには、フィルターを渡すクエリの中に記述してください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を指定すると、プッシュダウンできない外側のフィルターはローカルで適用される代わりに例外として拒否されます。
:::

`|` で列挙する必要がある複数のレプリカをサポートしています。例えば:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

<div id="usage-example">
  ## 使用例
</div>

MySQL でテーブルを作成します:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

通常の引数を使ってClickHouseにテーブルを作成します:

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

または、[named collections](/ja/operations/named-collections.md) を使用します:

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

MySQLテーブルからデータを取得:

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

<div id="mysql-settings">
  ## 設定
</div>

既定の設定は、接続の再利用すら行わないため、効率がよくありません。これらの設定により、サーバーが1秒あたりに実行できるクエリ数を増やせます。

<div id="connection-auto-close">
  ### `connection_auto_close`
</div>

クエリの実行後に接続を自動的に閉じるかどうかを指定します。つまり、接続の再利用を無効にします。

設定可能な値:

* 1 — 接続の自動クローズが許可され、接続の再利用は無効になります
* 0 — 接続の自動クローズは許可されず、接続の再利用は有効になります

デフォルト値: `1`.

<div id="connection-max-tries">
  ### `connection_max_tries`
</div>

フェイルオーバーを備えたプールの再試行回数を設定します。

設定可能な値:

* 正の整数。
* 0 — フェイルオーバーを備えたプールでは再試行を行いません。

デフォルト値: `3`。

<div id="connection-pool-size">
  ### `connection_pool_size`
</div>

接続プールのサイズです (すべての接続が使用中の場合、クエリは空きが出るまで待機します) 。

設定可能な値:

* 正の整数。

デフォルト値: `16`.

<div id="connection-wait-timeout">
  ### `connection_wait_timeout`
</div>

空き接続を待機する際のタイムアウト (秒単位) 。すでに `connection_pool_size` 個の接続がアクティブな場合に適用され、`0` の場合は待機しません。

設定可能な値:

* 正の整数。

デフォルト値: `5`。

<div id="connect-timeout">
  ### `connect_timeout`
</div>

接続タイムアウト (秒) 。

設定可能な値:

* 正の整数。

デフォルト値: `10`.

<div id="read-write-timeout">
  ### `read_write_timeout`
</div>

読み取り/書き込みタイムアウト (秒単位) 。

設定可能な値:

* 正の整数。

デフォルト値: `300`。

<div id="enable-compression">
  ### `enable_compression`
</div>

MySQL プロトコル接続で圧縮を有効にします。

デフォルト値: `false`。

この設定は以下に適用されます:

* `MySQL` テーブルエンジン;
* `MySQL` データベースエンジン;
* `mysql` テーブル関数;
* MySQL インテグレーションで使用される named collections。

有効にすると、ClickHouse はその接続で圧縮を使用するよう要求します。

例:

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

<div id="see-also">
  ## 関連項目
</div>

* [MySQL テーブル関数](../../../sql-reference/table-functions/mysql.md)
* [MySQL を Dictionary ソースとして使用する](/ja/sql-reference/statements/create/dictionary/sources/mysql)