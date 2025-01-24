---
slug: /ja/engines/table-engines/integrations/mysql
sidebar_position: 138
sidebar_label: MySQL
---

# MySQL テーブルエンジン

MySQL エンジンを使用すると、リモートの MySQL サーバーに保存されているデータに対して `SELECT` および `INSERT` クエリを実行することができます。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300 ]
;
```

[CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) クエリの詳細な説明を参照してください。

テーブル構造は元の MySQL テーブル構造と異なる場合があります：

- カラム名は元の MySQL テーブルと同じでなければなりませんが、これらのカラムの一部しか使用しないことができ、順序も任意です。
- カラムタイプは元の MySQL テーブルのものと異なる場合があります。ClickHouse は値を ClickHouse のデータタイプに[キャスト](../../../engines/database-engines/mysql.md#data_types-support)しようとします。
- [external_table_functions_use_nulls](../../../operations/settings/settings.md#external-table-functions-use-nulls) 設定は Nullable カラムの処理方法を定義します。デフォルト値: 1。0の場合、テーブル関数は Nullable カラムを作成せず、null の代わりにデフォルト値を挿入します。これは配列内の NULL 値にも適用されます。

:::note
MySQL テーブルエンジンは、現在 MacOS 用の ClickHouse ビルドでは利用できません([issue](https://github.com/ClickHouse/ClickHouse/issues/21191))
:::

**エンジンパラメータ**

- `host:port` — MySQL サーバーのアドレス。
- `database` — リモートデータベース名。
- `table` — リモートテーブル名。
- `user` — MySQL ユーザー。
- `password` — ユーザーパスワード。
- `replace_query` — `INSERT INTO` クエリを `REPLACE INTO` クエリに変換するフラグ。`replace_query=1` の場合、クエリが置き換えられます。
- `on_duplicate_clause` — `INSERT` クエリに追加される `ON DUPLICATE KEY on_duplicate_clause` 式。例： `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1` で、`on_duplicate_clause` は `UPDATE c2 = c2 + 1` です。`ON DUPLICATE KEY` 句で使用可能な `on_duplicate_clause` については [MySQL のドキュメント](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html)をご覧ください。`on_duplicate_clause` を指定するには、`replace_query` パラメータに `0` を渡す必要があります。`replace_query = 1` と `on_duplicate_clause` の両方を同時に渡すと、ClickHouse は例外を生成します。

引数は[名前付きコレクション](/docs/ja/operations/named-collections.md)を使用して渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。このアプローチは本番環境で推奨されます。

`=, !=, >, >=, <, <=` のようなシンプルな `WHERE` 条件は MySQL サーバー上で実行されます。

残りの条件と `LIMIT` サンプリング制約は、MySQL へのクエリが完了した後に ClickHouse でのみ実行されます。

複数のレプリカをサポートしており、`|` でリストする必要があります。例えば：

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

## 使用例 {#usage-example}

MySQL でテーブルを作成：

``` text
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

ClickHouse で引数を使ってテーブルを作成：

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

または[名前付きコレクション](/docs/ja/operations/named-collections.md)を使用：

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

MySQL テーブルからデータを取得：

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## 設定 {#mysql-settings}

デフォルト設定は非常に効率的ではなく、接続を再利用することすらしません。これらの設定により、サーバーによって実行されるクエリ数を秒単位で増やすことができます。

### connection_auto_close {#connection-auto-close}

クエリ実行後に接続を自動的にクローズし、接続の再利用を無効にすることを許可します。

可能な値：

- 1 — 自動クローズ接続が許可されているため、接続の再利用は無効です
- 0 — 自動クローズ接続は許可されていないため、接続の再利用は有効です

デフォルト値: `1`。

### connection_max_tries {#connection-max-tries}

フォールオーバー付きプールの再試行回数を設定します。

可能な値：

- 正の整数。
- 0 — フォールオーバー付きプールの再試行はありません。

デフォルト値: `3`。

### connection_pool_size {#connection-pool-size}

接続プールのサイズ (すべての接続が使用中の場合、クエリは接続が解放されるまで待機します)。

可能な値：

- 正の整数。

デフォルト値: `16`。

### connection_wait_timeout {#connection-wait-timeout}

空き接続を待つためのタイムアウト（秒単位）（すでに connection_pool_size のアクティブな接続がある場合）、0 - 待機しない。

可能な値：

- 正の整数。

デフォルト値: `5`。

### connect_timeout {#connect-timeout}

接続タイムアウト（秒単位）。

可能な値：

- 正の整数。

デフォルト値: `10`。

### read_write_timeout {#read-write-timeout}

読み取り/書き込みタイムアウト（秒単位）。

可能な値：

- 正の整数。

デフォルト値: `300`。

## 関連項目 {#see-also}

- [mysql テーブル関数](../../../sql-reference/table-functions/mysql.md)
- [Dictionary ソースとして MySQL を使用](../../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-mysql)
