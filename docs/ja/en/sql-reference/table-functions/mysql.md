---
description: 'リモートの MySQL サーバー に保存されているデータに対して `SELECT` および `INSERT` クエリを実行できます。'
sidebar_label: 'mysql'
sidebar_position: 137
slug: /sql-reference/table-functions/mysql
title: 'mysql'
doc_type: 'reference'
---

リモートの MySQL サーバー に保存されているデータに対して `SELECT` および `INSERT` クエリを実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## 引数
</div>

| 引数                    | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `host:port`           | MySQL サーバー のアドレスです。                                                                                                                                                                                                                                                                                                                                                                                                               |
| `database`            | リモート database の名前です。                                                                                                                                                                                                                                                                                                                                                                                                                |
| `table`               | リモート table の名前、またはそのまま MySQL に渡されるクエリです ([テーブル名の代わりにクエリを渡す](#passing-a-query)を参照) 。                                                                                                                                                                                                                                                                                                                                                 |
| `user`                | MySQL user です。                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `password`            | user の password です。                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `replace_query`       | `INSERT INTO` クエリを `REPLACE INTO` に変換する flag です。設定可能な値:<br />    - `0` - クエリは `INSERT INTO` として実行されます。<br />    - `1` - クエリは `REPLACE INTO` として実行されます。                                                                                                                                                                                                                                                                              |
| `on_duplicate_clause` | `INSERT` クエリに追加される `ON DUPLICATE KEY on_duplicate_clause` expression です。指定できるのは `replace_query = 0` の場合のみです (`replace_query = 1` と `on_duplicate_clause` を同時に渡すと、ClickHouse は例外を返します) 。<br />    例: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br />    ここでの `on_duplicate_clause` は `UPDATE c2 = c2 + 1` です。`ON DUPLICATE KEY` clause で使用できる `on_duplicate_clause` については、MySQL のドキュメントを参照してください。 |

引数は [named collections](/ja/operations/named-collections.md) を使って渡すこともできます。この場合、`host` と `port` は別々に指定する必要があります。この方法は本番環境での利用に推奨されます。

`=, !=, >, >=, <, <=` のような単純な `WHERE` clauses は、現在 MySQL サーバー 上で実行されます。

それ以外の conditions と `LIMIT` による sampling の制約は、MySQL へのクエリの完了後に ClickHouse でのみ実行されます。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを指定する
</div>

テーブル名の代わりに、3 番目の引数には、そのまま MySQL に渡される `SELECT` クエリを指定できます。結果として得られるテーブルの構造は、クエリ結果から推論されます。クエリは、サブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

これは、JOIN、集計、その他の処理を MySQL 側にプッシュダウンするのに便利です。このようなテーブルは読み取り専用であり、これに対する `INSERT` は許可されていません。同じ構文は [`MySQL`](/ja/engines/table-engines/integrations/mysql) テーブルエンジンでもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は ClickHouse によって解析され、サーバーに送信される前に MySQL 方言 (バッククォートによる識別子のクォート) で再シリアライズされます。そのため、有効な ClickHouse SQL である必要があります。ClickHouse が解析しない MySQL 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式のテキストは、そのまま MySQL に送信されます。

周囲の ClickHouse クエリに含まれる外側の `WHERE`、`LIMIT`、集計などは、渡されたクエリには**プッシュダウンされません**。これらは、クエリ結果全体をフェッチした後に ClickHouse で適用されます。MySQL から読み取るデータを制限するには、フィルターを渡すクエリ内に記述してください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を指定すると、プッシュダウンできない外側のフィルターはローカルで適用されず、代わりに例外として拒否されます。
:::

複数のレプリカをサポートしており、`|` で列挙する必要があります。例:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

OR

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

<div id="returned_value">
  ## 戻り値
</div>

元の MySQL テーブルと同じカラムを持つテーブルオブジェクト。

:::note
MySQL の一部のデータ型は、複数の ClickHouse の型にマッピングされる場合があります。これはクエリレベルの設定 [mysql&#95;datatypes&#95;support&#95;level](/ja/operations/settings/settings.md#mysql_datatypes_support_level) で制御できます。
:::

:::note
`INSERT` クエリで、テーブル関数 `mysql(...)` をカラム名リスト付きのテーブル名と区別するには、キーワード `FUNCTION` または `TABLE FUNCTION` を使用する必要があります。以下の例を参照してください。
:::”

<div id="examples">
  ## 例
</div>

MySQLのテーブル:

```text
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

ClickHouse からデータを取得する場合:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

または、[named collections](/ja/operations/named-collections.md) を使用します：

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

<div id="enable-compression">
  ### `enable_compression`
</div>

MySQL プロトコルの接続で圧縮を有効にします。

デフォルト値: `false`。

この設定は次に適用されます。

* `mysql` テーブル関数
* `MySQL` テーブルエンジン
* `MySQL` データベースエンジン
* MySQL インテグレーションで使用される named collections

有効にすると、ClickHouse は接続に対して圧縮を要求します。

例:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

置換と挿入：

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

MySQLテーブルからClickHouseテーブルへデータをコピーする:

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

あるいは、現在の最大 ID を基準に MySQL から増分バッチのみをコピーする場合:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

<div id="related">
  ## 関連
</div>

* [「MySQL」テーブルエンジン](../../engines/table-engines/integrations/mysql.md)
* [MySQLをDictionary ソースとして使用する](/ja/sql-reference/statements/create/dictionary/sources/mysql)
* [mysql&#95;datatypes&#95;support&#95;level](/ja/operations/settings/settings.md#mysql_datatypes_support_level)
* [mysql&#95;map&#95;fixed&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ja/operations/settings/settings.md#mysql_map_fixed_string_to_text_in_show_columns)
* [mysql&#95;map&#95;string&#95;to&#95;text&#95;in&#95;show&#95;columns](/ja/operations/settings/settings.md#mysql_map_string_to_text_in_show_columns)
* [mysql&#95;max&#95;rows&#95;to&#95;insert](/ja/operations/settings/settings.md#mysql_max_rows_to_insert)