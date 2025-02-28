---
slug: /ja/sql-reference/table-functions/mysql
sidebar_position: 137
sidebar_label: mysql
---

# mysql

リモートのMySQLサーバーに保存されているデータに対して`SELECT`および`INSERT`クエリを実行できるようにします。

**構文**

``` sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

**パラメーター**

- `host:port` — MySQLサーバーのアドレス。
- `database` — リモートデータベース名。
- `table` — リモートテーブル名。
- `user` — MySQLユーザー。
- `password` — ユーザーパスワード。
- `replace_query` — `INSERT INTO`クエリを`REPLACE INTO`に変換するフラグ。可能な値:
    - `0` - クエリは`INSERT INTO`として実行されます。
    - `1` - クエリは`REPLACE INTO`として実行されます。
- `on_duplicate_clause` — `ON DUPLICATE KEY on_duplicate_clause`が`INSERT`クエリに追加されます。`replace_query = 0`の場合にのみ指定できます（`replace_query = 1`と`on_duplicate_clause`を同時に渡すと、ClickHouseは例外を生成します）。
    例: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`
    ここでの`on_duplicate_clause`は`UPDATE c2 = c2 + 1`です。`ON DUPLICATE KEY`句で使用できる`on_duplicate_clause`については、MySQLのドキュメントを参照してください。

引数は、[named collections](/docs/ja/operations/named-collections.md)を使用して渡すこともできます。この場合、`host`と`port`は別々に指定する必要があります。このアプローチは、本番環境で推奨されます。

現在、`=, !=, >, >=, <, <=`のようなシンプルな`WHERE`句はMySQLサーバー上で実行されます。

残りの条件と`LIMIT`のサンプリング制約は、MySQLへのクエリが終了した後にClickHouseでのみ実行されます。

複数のレプリカをサポートしており、`|`でリストする必要があります。例：

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

または

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

**返される値**

元のMySQLテーブルと同じカラムを持つテーブルオブジェクト。

:::note
テーブル関数`mysql(...)`をカラム名のリストを持つテーブル名と区別するためには、`INSERT`クエリでキーワード`FUNCTION`または`TABLE FUNCTION`を使用する必要があります。以下の例を参照してください。
:::

**例**

MySQLでのテーブル:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

ClickHouseからのデータ選択:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

または[named collections](/docs/ja/operations/named-collections.md)を使用して:

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

``` text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

置換と挿入:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

``` text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

MySQLテーブルからClickHouseテーブルへのデータコピー:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

または、現在の最大IDに基づいてMySQLからのインクリメンタルバッチのみをコピーする場合:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) from mysql_copy);
```

**関連項目**

- [MySQLテーブルエンジン](../../engines/table-engines/integrations/mysql.md)
- [MySQLをDictionaryソースとして使用する](../../sql-reference/dictionaries/index.md#dictionary-sources#dicts-external_dicts_dict_sources-mysql)
