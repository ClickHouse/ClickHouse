---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: MySQL
---

# Mysql {#mysql}

MySQLエンジンでは、次の操作を実行できます `SELECT` リモートMySQLサーバーに格納されているデータに対するクエリ。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

の詳細な説明を参照してください [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) クエリ。

テーブル構造は、元のMySQLテーブル構造と異なる場合があります:

-   カラム名は元のMySQLテーブルと同じでなければなりませんが、これらのカラムの一部だけを任意の順序で使用できます。
-   列の型は、元のMySQLテーブルの型とは異なる場合があります。 ClickHouseは [キャスト](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) ClickHouseデータ型の値。

**エンジン変数**

-   `host:port` — MySQL server address.

-   `database` — Remote database name.

-   `table` — Remote table name.

-   `user` — MySQL user.

-   `password` — User password.

-   `replace_query` — Flag that converts `INSERT INTO` へのクエリ `REPLACE INTO`. もし `replace_query=1`、クエリが代入されます。

-   `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` に追加される式 `INSERT` クエリ。

    例: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`,ここで `on_duplicate_clause` は `UPDATE c2 = c2 + 1`. を参照。 [MySQLドキュメント](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) これを見つけるには `on_duplicate_clause` と使用できます `ON DUPLICATE KEY` 句。

    指定するには `on_duplicate_clause` 合格する必要があります `0` に `replace_query` パラメータ。 あなたが同時に合格した場合 `replace_query = 1` と `on_duplicate_clause`,ClickHouseは例外を生成します。

シンプル `WHERE` 次のような句 `=, !=, >, >=, <, <=` MySQLサーバー上で実行されます。

残りの条件と `LIMIT` サンプリング制約は、MySQLへのクエリが終了した後にのみClickHouseで実行されます。

## 使用例 {#usage-example}

MySQLのテーブル:

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

ClickHouseのテーブル、上で作成したMySQLテーブルからデータを取得する:

``` sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` sql
SELECT * FROM mysql_table
```

``` text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## も参照。 {#see-also}

-   [その ‘mysql’ テーブル関数](../../../sql-reference/table-functions/mysql.md)
-   [外部辞書のソースとしてMySQLを使用する](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[元の記事](https://clickhouse.com/docs/en/operations/table_engines/mysql/) <!--hide-->
